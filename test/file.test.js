var vows = require('vows');
var assert = require('assert');
var uuid = require('node-uuid');
var path = require('path');
var fs = require('fs');

var File = require('../lib/file');

vows.describe('File').addBatch({
  "2000 files": {
    topic: function() {
      var testdir = path.resolve('./files');
      if (!fs.existsSync(testdir)) {
        fs.mkdirSync(testdir)
      }
      var files=fs.readdirSync(testdir);
      for (var i=0; i<files.length; i++) {
        fs.unlinkSync(path.join(testdir,files[i]));
      }
      var data = new Buffer(16*1024);
      for (var i=0; i<2000; i++) {
        fs.writeFileSync(path.join(testdir, uuid.v1()+'.tst'), data);
      }
      this.callback(null, testdir);
    },
    "loading and reading": {
      topic: function(testdir) {
        var self = this;
        var filesRead = 0;
        var files=fs.readdirSync(testdir);
        for (var i=0; i<files.length; i++) {
          var stat = fs.statSync(path.join(testdir,files[i]));
          var file = new File(path.join(testdir,files[i]), stat.size, 0, function() {
            var buf = new Buffer(10240);
            file.read(buf, 0, 0, 10240, function(err, bytesRead) {
              assert.isNull(err);
              assert(bytesRead>0);
              console.log('read %d bytes from %s', bytesRead, file.path);

              filesRead++;
              if (filesRead==files.length) {
                self.callback();
              }
            });
          });
        }
      },
      "should give no error": function() {

      }
    }
  }
}).export(module);