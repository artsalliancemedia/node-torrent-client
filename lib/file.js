var async = require('async');

var fs = require('graceful-fs');
// change MAX_OPEN constant - the client should not use more than 512 file handles overall
fs.MAX_OPEN = 512;

var LOGGER = require('log4js').getLogger('file.js');


/**
 * The file object used by torrent to read/write from files
 * The torrent will create one for every file in the torrent upon initialization
 */
var File = function(filePath, length, offset, cb) {
  var self = this;

  self.path = filePath;
  self.length = length;
  self.offset = offset || 0;

  self.file = open(filePath);

  process.nextTick(cb);
};

File.PARTIAL = 'partial';
File.FULL = 'full';
File.NONE = 'none';

File.prototype.contains = function(pieceOffset, length) {
  var self = this;

  var fileEnd = self.offset + self.length;
  var pieceEnd = pieceOffset + length;
  
  if (pieceOffset >= self.offset && pieceEnd <= fileEnd) {
    return File.FULL;
  }
  if ((self.offset >= pieceOffset && self.offset <= pieceEnd)
      || (fileEnd >= pieceOffset && fileEnd <= pieceEnd)) {
    return File.PARTIAL;
  }
  return File.NONE;
};

File.prototype.read = function(buffer, bufferOffset, pieceOffset, length, cb) {
  var self = this;
  if (self.closing) {
    return cb(null, 0)
  }
  var match = self.contains(pieceOffset, length);
  if (match === File.PARTIAL || match === File.FULL) {
    var bounds = calculateBounds(self, pieceOffset, length);
    self.busy = true;
    // push the file read command to the queue
    fq.push(
      {cmd:'read', file:self.file, buffer:buffer, bufferOffset:bufferOffset, length:bounds.dataLength, position:bounds.offset},
      function(err, bytesRead) {
        self.busy = false;
        cb(err, bytesRead);
      }
    );
  }
  else {
    cb(null, 0);
  }
};

File.prototype.write = function(pieceOffset, data, cb) {
  var self = this;
  if (self.closing) {
    return cb(null, 0)
  }
  var match = self.contains(pieceOffset, data.length);
  if (match === File.PARTIAL || match === File.FULL) {
    var bounds = calculateBounds(self, pieceOffset, data.length);
    self.busy = true;
    // push the file write command to the queue
    fq.push(
      {cmd:'write', file:self.file, buffer:data, bufferOffset:bounds.dataOffset, length:bounds.dataLength, position:bounds.offset},
      function(err, bytesWritten) {
        self.busy = false;
        cb(err, bytesWritten);
      }
    );
  }
  else {
    cb(null, 0);
  }
};

File.prototype.close = function(cb) {
  cb = cb || function() {};
  var self = this;
  if (self.file) {
    self.closing = true;
    (function _close() {
      if (!self.busy) {
        close(self.file, function(err) {
          self.file = null;
          cb(err)
        })
      }
      else {
        setTimeout(_close, 50);
      }
    })();
  }
  else {
    cb()
  }
};

function calculateBounds(self, offset, length) {

  var dataStart = Math.max(self.offset, offset);
  var dataEnd = Math.min(self.offset+self.length, offset+length);

  return {
    dataOffset: dataStart - offset,
    dataLength: dataEnd - dataStart,
    offset: Math.max(offset-self.offset, 0)
  };
}


/**
 * The functions below are used by the File object to carry out read/write operations
 * instead of the fs ones. We impose a maximum number of open file descriptors,
 * but allow the torrents to open/create as many File objects as they need.
 * A file descriptor once opened is kept opened as long as the limit is not reached. If the limit
 * is reached and a File that doesn't have an opened descriptor attempts a read/write, the least
 * recently used file descriptor gets closed to make room and a file descriptor for the file
 * attempting the read/write is opened.
 * All file operations initiated by the File object are queued up/serialized using an async queue
 * with concurrency level set to 1 - nothing happens in parallel. This is reasonable for the
 * bit-torrent client scenario.
 */
var MAXOPEN = 256; // max number of file descriptors allowed
var fds = {} // holds the descriptors by file path
var fdcount = 0; // count of open file descriptors

// async queue for file operations
var fq = async.queue(function (task, cb) {
  if (task.cmd==='read') {
    read(task.file, task.buffer, task.bufferOffset, task.length, task.position, cb);
  }
  if (task.cmd==='write') {
    write(task.file, task.buffer, task.bufferOffset, task.length, task.position, cb);
  }
}, 1); // concurrency level = 1

function open(path) {
  // return an internal "file" object instead of the fd
  return { path: path, fd: null }
}

function close(file, cb) {
  if (file.fd) {
    // close the file descriptor if we have one
    fs.close(file.fd.fd, function(err) {
      if (err) {
        cb(err)
      }
      else {
        // clear the fd and decrement fd count
        fdcount--;
        delete fds[file.path];
        file.fd = null;
        cb()
      }
    })
  }
  else {
    cb()
  }
}

function read(file, buffer, offset, length, position, cb) {
  // request a fd descriptor
  getfd(file, function(err, fd) {
    if (err) {
      LOGGER.error('failed to read file', file.path, err);
      cb(err);
    }
    else {
      // update last used timestamp on the fd
      fd.lu = Date.now();
      // read data
      fs.read(fd.fd, buffer, offset, length, position, cb);
    }
  });
}

function write(file, buffer, offset, length, position, cb) {
  getfd(file, function(err, fd) {
    if (err) {
      LOGGER.error('failed to write to file', file.path, err);
      cb(err);
    }
    else {
      // update last used timestamp on the fd
      fd.lu = Date.now();
      // write data
      fs.write(fd.fd, buffer, offset, length, position, cb);
    }
  });
}

function getfd(file, cb) {

  // opens the file, calls back with the file descriptor
  function _open() {
    fs.exists(file.path, function(exists) {
      var flag = 'r+';
      if (!exists) {
        flag = 'w+';
      }
      fs.open(file.path, flag, 0666, function(err, fd) {
        if (err) {
          cb(err);
        }
        else {
          fdcount++;
          file.fd = { fd: fd, file: file }
          fds[file.path] = file.fd;
          cb(null, file.fd);
        }
      });
    });
  }

  // close least recently used file handle
  function _closelru(done) {

    var lru = null, lrukey;
    // find least recently used file handle
    for (var key in fds) {
      if (!lru || fds[key].lu < lru.lu) {
        lru = fds[key];
        lrukey = key;
      }
    }
    if (lru) {
      // close the file handle
      fs.close(lru.fd, function(err) {
        if (err) {
          done(err)
        }
        else {
        // clear the fd and decrement fd count
          fdcount--;
          delete fds[lrukey];
          lru.file.fd = null;
          done()
        }
      })
    }
    else {
      done()
    }
  }

  // if we already have a file descriptor, just return it
  if (file.fd) {
    cb(null, file.fd);
  }
  else {
    if (fdcount>=MAXOPEN) {
       // if the fd limit has been reached, close the last recently used fd
      _closelru(function(err) {
        if (err) {
          cb(err);
        }
        else {
          // ready to open a new fd
          _open();
        }
      });
    }
    else {
       // we're under the fd limit, ok to open a new one
      _open();
    }
  }
}

module.exports = File;
