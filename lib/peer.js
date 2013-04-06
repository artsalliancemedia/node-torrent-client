
var net = require('net');
var tls = require('tls');
var util = require('util');

var ProcessUtils = require('./util/processutils');
var BitField = require('./util/bitfield');
var BufferUtils = require('./util/bufferutils');
var EventEmitter = require('events').EventEmitter;
var Message = require('./message');
var OverflowList = require('./util/overflowlist');
var Piece = require('./piece');

var BITTORRENT_HEADER = new Buffer("\x13BitTorrent protocol\x00\x00\x00\x00\x00\x00\x00\x00", "binary");
var KEEPALIVE_PERIOD = 30; // secs
var MAX_REQUESTS = 50;
var FLUSH_BUFFER_SIZE = 128; // Megabytes

var CONNECTION_TIMEOUT = 35; // seconds
var MESSAGE_TIMEOUT = 65; // seconds
var UNCHOKE_TIMEOUT = 5; // seconds

var LOGGER = require('log4js').getLogger('peer.js');

var Peer = function(/* stream */ /* or */ /* peer_id, address, port, torrent */) {
  EventEmitter.call(this);

  this.choked = true;
  this.data = new Buffer(0);
  this.drained = true;
  this.initialised = false;
  this.interested = false;
  this.messages = [];
  this.toSend = [];
  this.toFlush = [];
  this.pieces = {};
  this.numRequests = 0;
  this.requests = {};
  this.requestsCount = {};
  this.stream = null;
  this.handshake = false;

  this.downloaded = 0;
  this.uploaded = 0;
  this.downloadedHistory = [];
  this.downloadRates = [];
  this.currentDownloadRate = 0;
  this.uploadedHistory = [];
  this.uploadRates = [];
  this.currentUploadRate = 0;

  this.running = false;
  this.processing = false;

  if (arguments.length === 1) {
    this.stream = arguments[0];
    this.address = this.stream.remoteAddress;
    this.port = this.stream.remotePort;
  } else {
    this.peerId = arguments[0];
    this.address = arguments[1];
    this.port = arguments[2];
    this.setTorrent(arguments[3]);
  }

  this.connect();

  var self = this;

  setTimeout(function() { forceUpdateRates(self) }, 1000);
};
util.inherits(Peer, EventEmitter);

Peer.prototype.connect = function() {
  var self = this;

  if (self.stream === null) {
    LOGGER.info('Connecting to peer at ' + self.address + ' on ' + self.port + ' using ' + self.torrent.connectionSettings.type);

    var provider = { tcp:net, tls:tls } [self.torrent.connectionSettings.type];
    var connectOptions = {
      host: self.address,
      port: self.port
    };
    // _.defaults(connectOptions, self.torrent.connectionSettings.options || {});
    var o = self.torrent.connectionSettings.options || {};
    Object.getOwnPropertyNames(o)
      .forEach(function (name) {
        connectOptions[name] = connectOptions[name] || o[name];
    });
    self.stream = provider.connect(connectOptions, function() {
      if (self.torrent.authorizer) {
        self.torrent.authorizer.authorizeOutgoing(connectOptions, self.stream, function(authorized) {
          if (authorized) {
            onConnect(self);
          }
          else {
            self.stream.destroy()
          }
        })
      }
      else {
        onConnect(self);
      }
    });
  }

  //self.stream.setNoDelay(false);
  //self.stream.setKeepAlive(true, 1000);

  self.stream.on('data', function(data) {onData(self, data);});
  self.stream.on('drain', function() {onDrain(self);});
  self.stream.on('end', function() {onEnd(self);});
  self.stream.on('error', function(e) {onError(self, e);});

  setTimeout(function() { connectionTimeoutCheck(self) }, 1e3);

};

function forceUpdateRates(self) {

  updateRates(self, 'down');
  updateRates(self, 'up');

  if (!self.disconnected) {
    setTimeout(function() { forceUpdateRates(self) }, 1000);
  }
}

Peer.prototype.disconnect = function(message, reconnectTimeout) {
  LOGGER.info('Peer.disconnect [' + this.getIdentifier() + '] message =', message);
  this.disconnected = true;
  if (this.stream) {
    this.stream.removeAllListeners();
    this.stream.destroy();
    this.stream = null;
  }
  if (this.keepAliveId) {
    clearInterval(this.keepAliveId);
    delete this.keepAliveId;
  }
  for (var index in this.pieces) {
    var piece = this.pieces[index];
    var requests = this.requests[index];
    if (requests) {
      for (var reqIndex in requests) {
        piece.cancelRequest(requests[reqIndex]);
      }
    }
    piece.resetRequests();
    removePieceListeners(this, piece);
  }

  this.requests = {};
  this.requestsCount = {};
  this.numRequests = 0;

  this.messages = [];
  this.toSend = [];

  this.emit(Peer.DISCONNECT);

  if (this.torrent) {
    this.bitfield = new BitField(this.torrent.bitfield.length);
  }

  this.data = new Buffer(0);

  this.initialised = false;
  this.handshake = false;

  this.interested = false;
  this.choked = true;

  if (reconnectTimeout) {
    var self = this;
    setTimeout(function() {self.connect();}, reconnectTimeout);
  }
  else {
    this.emit(Peer.DONE);
  }

};

Peer.prototype.getIdentifier = function() {
  return Peer.getIdentifier(this);
};

removePieceListeners = function(self, piece) {
  var listeners = piece.listeners(Piece.COMPLETE).slice(0);
  for (var lidx=0; lidx<listeners.length; lidx++) {
    var listener = listeners[lidx]['listener'];
    if (listener === self.onPieceComplete) {
      piece.removeListener(Piece.COMPLETE, listener);
    }
  }
}

function onPieceComplete(piece) {
  var self = this;

  if (!self.pieces[piece.index]) {
    LOGGER.warn('got piece COMPLETE for piece ' + piece.index + ' but piece is missing');
    return;
  }

  self.toFlush.push(piece);
  delete self.pieces[piece.index];
}

Peer.prototype.requestPiece = function(piece) {
  var self = this;

  if (!piece) {
    return;
  }

  var writeBufferFull = self.toFlush.length*self.torrent.pieceLength/1024 > FLUSH_BUFFER_SIZE*1024;

  // if requesting a new piece and the flush buffer is full, postpone request
  if (!self.pieces[piece.index] && writeBufferFull)
  {
    LOGGER.warn('Peer [' + self.getIdentifier() + '] requestPiece ' + piece.index + ' buffer overload, throttling');
    setTimeout(function() {
      //LOGGER.debug('peer overloaded, now re-requesting piece ' + piece.index);
      self.requestPiece(piece);
    }, 500);
    return;
  }

  if (!self.pieces[piece.index]) {
    LOGGER.debug('Peer [' + self.getIdentifier() + '] added ' + piece.index);

    self.pieces[piece.index] = piece;
    self.requests[piece.index] = {};

    // need one instance of onPieceComplete, attached to self
    if (!self.onPieceComplete) {
      self.onPieceComplete = onPieceComplete.bind(self);
    }
    removePieceListeners(self, piece);
    piece.once(Piece.COMPLETE, self.onPieceComplete);
  }

  var nextChunk;
  while (self.numRequests < MAX_REQUESTS && (nextChunk=piece.nextChunk()))
  {
    //LOGGER.debug('Peer.requestPiece [' + self.getIdentifier() + '] requesting piece ' + piece.index + ', begin ' + nextChunk.begin + ', length ' + nextChunk.length);
    self.requests[piece.index][nextChunk.begin] = new Date();
    var msgBuffer = new Buffer(12);
    msgBuffer.writeInt32BE(piece.index, 0, true);
    msgBuffer.writeInt32BE(nextChunk.begin, 4, true);
    msgBuffer.writeInt32BE(nextChunk.length, 8, true);
    var message = new Message(Message.REQUEST, msgBuffer);
    self.sendMessage(message);
    self.requestsCount[piece.index] = (self.requestsCount[piece.index] || 0) + 1;
    self.numRequests++;
  }

  if (self.numRequests < MAX_REQUESTS && !writeBufferFull) {
    ProcessUtils.nextTick(function() { self.emit(Peer.READY) });
  }
};

Peer.prototype.sendMessage = function(message) {
  var self = this;
  self.messages.push(message);
  if (!self.running) {
    self.running = true;
    ProcessUtils.nextTick(function(){nextMessage(self)});
  }
};

Peer.prototype.setAmInterested = function(interested) {
  var self = this;
  if (!self.disconnected && self.stream) {
    if (interested && !self.amInterested) {
      self.sendMessage(new Message(Message.INTERESTED));
      LOGGER.info('Sent INTERESTED to ' + self.stream.remoteAddress + ':' + self.stream.remotePort);
      self.amInterested = true;
      if (!self.choked) {
        self.emit(Peer.READY);
      }
    } else if (!interested && self.amInterested) {
      self.sendMessage(new Message(Message.UNINTERESTED));
      LOGGER.info('Sent UNINTERESTED to ' + self.stream.remoteAddress + ':' + self.stream.remotePort);
      self.amInterested = false;
    }
  }
};

Peer.prototype.setTorrent = function(torrent) {
  var self = this;
  var stream = self.stream;
  this.torrent = torrent;
  this.torrent.addPeer(this);
  this.bitfield = new BitField(torrent.bitfield.length);
  if (this.stream && !this.initialised) {
    doHandshake(this);
    this.initialised = true;
    this.sendMessage(new Message(Message.BITFIELD, this.torrent.bitfield.toBuffer()));
    LOGGER.info('Sent BITFIELD to ' + stream.remoteAddress + ':' + stream.remotePort);
    this.sendMessage(new Message(Message.UNCHOKE));
    LOGGER.info('Sent UNCHOKE to ' + stream.remoteAddress + ':' + stream.remotePort);
  }
};

function doHandshake(self) {
  var stream = self.stream;
  stream.write(BITTORRENT_HEADER);
  stream.write(self.torrent.infoHash);
  stream.write(self.torrent.clientId);
  this.handshake = true;
  LOGGER.info('Sent HANDSHAKE to ' + stream.remoteAddress + ':' + stream.remotePort);
}

function handleHandshake(self) {
  var data = self.data;
  if (data.length < 68) {
    // Not enough data.
    return;
  }
  if (!BufferUtils.equal(BITTORRENT_HEADER.slice(0, 20), data.slice(0, 20))) {
    self.disconnect('Invalid handshake. data = ' + data.toString('binary'));
  } else {
    var infoHash = data.slice(28, 48);
    self.peerId = data.toString('binary', 48, 68);
    LOGGER.info('Received HANDSHAKE from ' + self.stream.remoteAddress + ':' + self.stream.remotePort);
    self.data = BufferUtils.slice(data, 68);

    if (self.torrent) {
      self.initialised = true;
      self.running = true;
      nextMessage(self);
      processData(self);
      self.emit(Peer.CONNECT);
    } else {
      self.emit(Peer.CONNECT, infoHash);
    }
  }
}

function nextMessage(self) {
  if (!self.disconnected && self.initialised) {
    (function next() {
      if (self.messages.length === 0) {
        self.running = false;
        setKeepAlive(self);
      } else {
        if (!self.stream) {
          self.connect();
        } else {
          if (self.keepAliveId) {
            clearInterval(self.keepAliveId);
            delete self.keepAliveId;
          }
          while (self.messages.length > 0) {
            var message = self.messages.shift();
            // TODO: review
            // sometimes writeTo is called here after the stream is closed from the other side
            // or after it errors out and fails
            try {
              message.writeTo(self.stream);
            }
            catch(err) {
              LOGGER.warn('Peer [' + self.getIdentifier() + '] failed to send message: ' + err);
            }
          }
          next();
        }
      }
    })();
  }
}

function onConnect(self) {
  self.disconnected = false;
  if (self.torrent) {
    if (!self.handshake) {
      doHandshake(self);
    } else {
      self.running = true;
      nextMessage(self);
    }
  }
}

function onData(self, data) {
  self.gotDataOn = Date.now();

  self.data = BufferUtils.concat(self.data, data);
  if (!self.initialised) {
    handleHandshake(self);
  } else {
    if (!self.processing) {
        processData(self);
    }
  }
}

function onDrain(self) {
  self.drained = true;
}

function onEnd(self) {
  LOGGER.debug('Peer [' + self.getIdentifier() + '] received end');
  self.stream = null;
  if (self.amInterested) {
    // LOGGER.debug('Peer [' + self.getIdentifier() + '] after end continuing');
    // self.choked = false;
    // self.emit(Peer.READY);
    self.disconnect('after end, reconnect', 5000);
  } else {
    self.disconnect('stream ended and no interest');
  }
}

function onError(self, e) {
  LOGGER.debug('Peer [' + self.getIdentifier() + '] connection error');

  self.disconnect(e.message);
}

function onConnectionTimeout(self) {
  LOGGER.debug('Peer [' + self.getIdentifier() + '] timed-out');

  if (self.stream) {
    self.stream.end();
  }

  if (self.amInterested) {
    self.disconnect('after timeout, reconnect', 5000);
  } else {
    self.disconnect('peer timed-out and no interest');
  }
}

function onMessageTimeout(self) {
  LOGGER.debug('Peer [' + self.getIdentifier() + '] message time-out');

  if (self.stream) {
    self.stream.end();
  }

  self.disconnect('no messages');
}

function sendData(self) {
  var retry = false;
  (function next() {
    if (self.toSend.length > 0) {
      var message = self.toSend.shift();
      var index = message.payload.readInt32BE(0, true);
      var begin = message.payload.readInt32BE(4, true);
      var length = message.payload.readInt32BE(8, true);

      self.torrent.requestChunk(index, begin, length, function(err, data) {
        if (err) {
            if (err.code===Piece.ERR_FILEBUSY) {
              LOGGER.warn('Peer [' + self.getIdentifier() + '] sendData file busy');
              retry = true;
              self.toSend.push(message);
            }
            else {
              LOGGER.error('Failed to read file chunk: ' + err);
              throw err;
            }
        }
        else {
          if (data) {
            var msgBuffer = new Buffer(8+data.length);
            msgBuffer.writeInt32BE(index, 0, true);
            msgBuffer.writeInt32BE(begin, 4, true);
            data.copy(msgBuffer, 8);
            self.sendMessage(new Message(Message.PIECE, msgBuffer));
            self.uploaded += data.length;
            updateRates(self, 'up');
          } else {
            LOGGER.debug('No data found for request, index = ' + index + ', begin = ' + begin);
          }
          ProcessUtils.nextTick(next);
        }
      });
    }
    else {
      self.sending = false;
      if (retry) {
        setTimeout(function() {
          if (!self.sending) {
            self.sending = true;
            sendData(self);
          }
        }, 10);
      }
    }
  })();
}

function flushData(self) {
  var retry = false;
  //LOGGER.debug('Peer [' + self.getIdentifier() + '] flushData in progress');
  (function next() {
    if (self.toFlush.length > 0) {
      var piece = self.toFlush.shift();
      //LOGGER.debug('Peer [' + self.getIdentifier() + '] flushData flushing piece ' + piece.index);
      piece.flushData(function(err) {
        if (err) {
          if (err.code===Piece.ERR_FILEBUSY) {
            //LOGGER.info('Peer [' + self.getIdentifier() + '] flushData file busy, postpone');
            retry = true;
            self.toFlush.push(piece);
          }
          else {
            LOGGER.error('Failed to write data to disk: ' + err);
            throw err;
          }
        }
        ProcessUtils.nextTick(next);
      });
    }
    else {
      //LOGGER.debug('Peer [' + self.getIdentifier() + '] flushData finished.');
      self.flushing = false;
      if (retry) {
        setTimeout(function() {
          if (!self.flushing) {
            self.flushing = true;
            flushData(self);
          }
        }, 50);
      }
    }
  })();
}

function processData(self) {

    var offset = 0;

    self.processing = true;
    //console.log('processing...');

    function done() {
        if (offset > 0) {
          self.data = self.data.slice(offset);
          //console.log('processed ' + offset + ' bytes, ' + self.data.length + ' bytes remaining');
        }
        self.processing = false;
    }

    do {
      //console.log('offset is ' + offset);
      if (self.data.length-offset >= 4)
      {
          //console.log('have at least 4 bytes, getting message length');
          var messageLength = self.data.readInt32BE(offset, true);
          offset += 4;
          //console.log('message length is ' + messageLength);
          if (messageLength === 0)
          {
              // Keep alive
              LOGGER.debug('Peer [' + self.getIdentifier() + '] received keep alive');
          }
          else if (self.data.length-offset >= messageLength)
          {
              self.gotMessageOn = Date.now();

              // Have everything we need to process a message
              var code = self.data[offset];
              var payload = messageLength > 1 ? self.data.slice(offset+1, offset+messageLength) : null;
              offset += messageLength;

              var message = new Message(code, payload);
              switch (message.code) {

                case Message.CHOKE:
                  LOGGER.info('Received CHOKE from ' + Peer.getIdentifier(self));
                  self.choked = true;
                  self.emit(Peer.CHOKED);
                  break;

                case Message.UNCHOKE:
                  LOGGER.info('Received UNCHOKE from ' + Peer.getIdentifier(self));
                  self.choked = false;
                  if (self.amInterested) {
                    self.emit(Peer.READY);
                  }
                  break;

                case Message.INTERESTED:
                  LOGGER.info('Received INTERESTED from ' + Peer.getIdentifier(self));
                  self.interested = true;
                  break;

                case Message.UNINTERESTED:
                  LOGGER.info('Received UNINTERESTED from ' + Peer.getIdentifier(self));
                  self.interested = false;
                  break;

                case Message.HAVE:
                  LOGGER.debug('Received HAVE from ' + Peer.getIdentifier(self));
                  var piece = message.payload.readInt32BE(0, true);
                  self.bitfield.set(piece);
                  self.emit(Peer.UPDATED);
                  break;

                case Message.BITFIELD:
                  LOGGER.info('Received BITFIELD from ' + Peer.getIdentifier(self));
                  self.bitfield = new BitField(message.payload, self.torrent.bitfield.length); // TODO: figure out nicer way of handling bitfield lengths
                  self.emit(Peer.UPDATED);
                  break;

                case Message.REQUEST:
                  LOGGER.debug('Received REQUEST from ' + Peer.getIdentifier(self));
                  self.toSend.push(message);
                  if (!self.sending) {
                    self.sending = true;
                    setTimeout(function() {sendData(self)}, 10);
                  }
                  break;

                case Message.PIECE:
                  self.numRequests--;
                  var index = message.payload.readInt32BE(0, true);
                  var begin = message.payload.readInt32BE(4, true);
                  var data = message.payload.slice(8);

                  //LOGGER.debug('Peer [' + self.getIdentifier() + '] got piece ' + index + ', begin ' + begin + ', length ' + data.length);

                  var piece = self.pieces[index];
                  if (piece) {
                      piece.setData(data, begin);
                  } else {
                    LOGGER.info('chunk received for inactive piece');
                  }

                  if (self.requests[index] && self.requests[index][begin]) {
                    var requestTime = new Date() - self.requests[index][begin];
                    self.downloaded += data.length;
                    delete self.requests[index][begin];
                    self.requestsCount[index]--;
                    updateRates(self, 'down');
                    //LOGGER.debug('Downloading piece: ' + Peer.getIdentifier(self) + ', download rate = ' + (self.currentDownloadRate / 1024) + 'Kb/s');
                  }

                  if (piece && !piece.isComplete && self.requests[index]) {
                    if ((self.requestsCount[index]<1) || (MAX_REQUESTS-self.numRequests>5))
                    {
                      //LOGGER.debug('got a chunk, requesting more for piece ' + piece.index);
                      self.requestPiece(piece);
                    }
                  }

                  if (piece && piece.isComplete) {
                    if (!self.flushing) {
                      self.flushing = true;
                      setTimeout(function() {flushData(self)}, 10);
                    }
                    self.emit(Peer.READY);
                  }

                  break;

                case Message.CANCEL:
                  //console.log('received CANCEL message');
                  LOGGER.info('Ignoring CANCEL');
                  break;

                case Message.PORT:
                  //console.log('received PORT message');
                  LOGGER.info('Ignoring PORT');
                  break;

                default:
                  LOGGER.warn('Peer [' + self.getIdentifier() + '] received unknown message, disconnecting. ');
                  self.disconnect('Unknown message received.');
                  // stop processing
                  done();
                  return;
              }
          }
          else {
            // not enough data, stop processing until more data arrives
            offset -= 4;
            done();
            return;
          }
      }
      else {
        // not enough data to read the message length, stop processing until more data arrives
        done();
        if (!self.running) {
          self.running = true;
          ProcessUtils.nextTick(function(){nextMessage(self)});
        }
        return;
      }
    } while (true);
}

function setKeepAlive(self) {
  if (!self.keepAliveId) {
    self.keepAliveId = setInterval(function() {
  	  LOGGER.debug('keepAlive tick');
      if (self.stream && self.stream.writable) {
        var message = new Message(Message.KEEPALIVE);
        message.writeTo(self.stream);
      } else {
        clearInterval(self.keepAliveId);
      }
    }, KEEPALIVE_PERIOD*1e3);
  }
}

function connectionTimeoutCheck(self) {

  if (!self.disconnected) {
    if (!self.gotDataOn || Date.now()-self.gotDataOn>CONNECTION_TIMEOUT*1e3) {
      onConnectionTimeout(self);
      return
    }
    if (self.initialised) {
      if (!self.gotMessageOn || Date.now()-self.gotMessageOn>MESSAGE_TIMEOUT*1e3) {
        onMessageTimeout(self);
        return
      }
      if (self.gotMessageOn && Date.now()-self.gotMessageOn>UNCHOKE_TIMEOUT*1e3) {
        self.sendMessage(new Message(Message.UNCHOKE));
        LOGGER.info('No messages, sent UNCHOKE to ' + self.stream.remoteAddress + ':' + self.stream.remotePort);
      }
    }
    setTimeout(function() {connectionTimeoutCheck(self)}, 1e3);
  }
}

// calculate weighted average upload/download rate
function calculateRate(self, kind) {
  var isUpload = (kind=='up');

  var rates = isUpload ? self.uploadRates : self.downloadRates;

  // take the last recorded rate
//  var rate = (rates.length > 0) ? rates[rates.length-1].value : 0

  // calculate weighted average rate
  //var decayFactor = 0.13863;
  var rateSum = 0, weightSum = 0;
  for (var idx=0; idx<rates.length; idx++) {
    //var age = rates[idx].ts-rates[0].ts;
    var weight = 1; //Math.exp(-decayFactor*age/1000);
    rateSum += rates[idx].value * weight;
    weightSum += weight;
  }
  var rate = (rates.length>0) ? (rateSum/weightSum) : 0;

  if (rate > 0) {
    LOGGER.info('Peer [' + self.getIdentifier() + '] ' + kind + 'loading at ' + rate);
  }

  if (isUpload) {
    self.currentUploadRate = rate;
  }
  else {
    self.currentDownloadRate = rate;
  }
}

function updateRates(self, kind) {
  var isUpload = (kind=='up');

  var history = isUpload ? self.uploadedHistory : self.downloadedHistory;
  var rates = isUpload ? self.uploadRates : self.downloadRates;

  var now = Date.now();
  var bytes = isUpload ? self.uploaded : self.downloaded;
  history.push({ts: now, value: bytes});

  if (history.length > 1) {
    var start = history[0].ts;
    if (now-start > 1*1000) {
      // calculate a new rate and remove first entry from history
      var rate = (bytes-history.shift().value)/(now-start)*1000;
      rates.push({ts: now, value: rate});
      // throw out any rates that are too old to be of interest
      while((rates.length>1) && (now-rates[0].ts>3*1000)) {
        rates.shift();
      }
      // re-calculate current upload/download rate
      calculateRate(self, kind);
    }
    else {
      // just want to keep the first and the last entry in history
      history.splice(1,1);
    }
  }
}

Peer.CHOKED = 'choked';
Peer.CONNECT = 'connect';
Peer.DISCONNECT = 'disconnect';
Peer.READY = 'ready';
Peer.UPDATED = 'updated';
Peer.DONE = 'done';

Peer.getIdentifier = function(peer) {
  return (peer.address || peer.ip) + ':' + peer.port;
}

module.exports = Peer;
