'use strict';

var _ = require('lodash');
var async = require('async');
var child_process = require('child_process'); // eslint-disable-line camelcase
var request = require('request');
var should = require('should');

var nsq = require('../src/nsq');

var temp = require('temp').track();

var TCP_PORT = 4150;
var HTTP_PORT = 4151;

var startNSQD = function startNSQD(dataPath) {
  var additionalOptions = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
  var callback = arguments[2];

  var options = {
    'http-address': '127.0.0.1:' + HTTP_PORT,
    'tcp-address': '127.0.0.1:' + TCP_PORT,
    'broadcast-address': '127.0.0.1',
    'data-path': dataPath,
    'tls-cert': './test/cert.pem',
    'tls-key': './test/key.pem'
  };

  _.extend(options, additionalOptions);

  // Convert to array for child_process.
  options = Object.keys(options).map(function (option) {
    return ['-' + option, options[option]];
  });

  var process = child_process.spawn('nsqd', _.flatten(options), {
    stdio: ['ignore', 'ignore', 'ignore']
  });

  process.on('error', function (err) {
    throw err;
  });

  var retryOptions = { times: 10, interval: 50 };
  var liveliness = function liveliness(callback) {
    request('http://localhost:' + HTTP_PORT + '/ping', function (err, res, body) {
      if (err || res.statusCode != 200) {
        return callback(new Error('nsqd not ready'));
      }
      callback();
    });
  };

  async.retry(retryOptions, liveliness, function (err) {
    callback(err, process);
  });
};

var topicOp = function topicOp(op, topic, callback) {
  var options = {
    method: 'POST',
    uri: 'http://127.0.0.1:' + HTTP_PORT + '/' + op,
    qs: {
      topic: topic
    }
  };

  request(options, function (err) {
    return callback(err);
  });
};

var createTopic = _.partial(topicOp, 'topic/create');
var deleteTopic = _.partial(topicOp, 'topic/delete');

// Publish a single message via HTTP
var publish = function publish(topic, message) {
  var callback = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : function () {};

  var options = {
    uri: 'http://127.0.0.1:' + HTTP_PORT + '/pub',
    method: 'POST',
    qs: {
      topic: topic
    },
    body: message
  };

  request(options, function (err) {
    return callback(err);
  });
};

describe('integration', function () {
  var nsqdProcess = null;
  var reader = null;

  beforeEach(function (done) {
    async.series([
    // Start NSQD
    function (callback) {
      temp.mkdir('/nsq', function (err, dirPath) {
        if (err) return callback(err);

        startNSQD(dirPath, {}, function (err, process) {
          nsqdProcess = process;
          callback(err);
        });
      });
    },
    // Create the test topic
    function (callback) {
      createTopic('test', callback);
    }], done);
  });

  afterEach(function (done) {
    async.series([function (callback) {
      reader.on('nsqd_closed', function (nsqdAddress) {
        callback();
      });
      reader.close();
    }, function (callback) {
      deleteTopic('test', callback);
    }, function (callback) {
      nsqdProcess.on('exit', function (err) {
        callback(err);
      });
      nsqdProcess.kill('SIGKILL');
    }], function (err) {
      // After each start, increment the ports to prevent possible conflict the
      // next time an NSQD instance is started. Sometimes NSQD instances do not
      // exit cleanly causing odd behavior for tests and the test suite.
      TCP_PORT = TCP_PORT + 50;
      HTTP_PORT = HTTP_PORT + 50;

      reader = null;
      done(err);
    });
  });

  describe('stream compression and encryption', function () {
    var optionPermutations = [{ deflate: true }, { snappy: true }, { tls: true, tlsVerification: false }, { tls: true, tlsVerification: false, snappy: true }, { tls: true, tlsVerification: false, deflate: true }];

    optionPermutations.forEach(function (options) {
      var compression = ['deflate', 'snappy'].filter(function (key) {
        return key in options;
      }).map(function (key) {
        return key;
      });

      compression.push('none');

      // Figure out what compression is enabled
      var description = 'reader with compression (' + compression[0] + ') and tls (' + (options.tls != null) + ')';

      describe(description, function () {
        it('should send and receive a message', function (done) {
          var topic = 'test';
          var channel = 'default';
          var message = 'a message for our reader';

          publish(topic, message);

          reader = new nsq.Reader(topic, channel, _.extend({ nsqdTCPAddresses: ['127.0.0.1:' + TCP_PORT] }, options));

          reader.on('message', function (msg) {
            should.equal(msg.body.toString(), message);
            msg.finish();
            done();
          });

          reader.on('error', function () {});

          reader.connect();
        });

        it('should send and receive a large message', function (done) {
          var topic = 'test';
          var channel = 'default';
          var message = _.range(0, 100000).map(function () {
            return 'a';
          }).join('');

          publish(topic, message);

          reader = new nsq.Reader(topic, channel, _.extend({ nsqdTCPAddresses: ['127.0.0.1:' + TCP_PORT] }, options));

          reader.on('message', function (msg) {
            should.equal(msg.body.toString(), message);
            msg.finish();
            done();
          });

          reader.on('error', function () {});

          reader.connect();
        });
      });
    });
  });

  describe('end to end', function () {
    var topic = 'test';
    var channel = 'default';
    var writer = null;
    reader = null;

    beforeEach(function (done) {
      writer = new nsq.Writer('127.0.0.1', TCP_PORT);
      writer.on('ready', function () {
        reader = new nsq.Reader(topic, channel, {
          nsqdTCPAddresses: ['127.0.0.1:' + TCP_PORT]
        });
        reader.on('nsqd_connected', function (addr) {
          return done();
        });
        reader.connect();
      });

      writer.on('error', function () {});
      writer.connect();
    });

    afterEach(function () {
      writer.close();
    });

    it('should send and receive a string', function (done) {
      var message = 'hello world';
      writer.publish(topic, message, function (err) {
        if (err) done(err);
      });

      reader.on('error', function (err) {
        console.log(err);
      });

      reader.on('message', function (msg) {
        msg.body.toString().should.eql(message);
        msg.finish();
        done();
      });
    });

    it('should send and receive a Buffer', function (done) {
      var message = new Buffer([0x11, 0x22, 0x33]);
      writer.publish(topic, message);

      reader.on('error', function () {});

      reader.on('message', function (readMsg) {
        for (var i = 0; i < readMsg.body.length; i++) {
          should.equal(readMsg.body[i], message[i]);
        }
        readMsg.finish();
        done();
      });
    });

    it('should not receive messages when immediately paused', function (done) {
      setTimeout(done, 50);

      // Note: because NSQDConnection.connect() does most of it's work in
      // process.nextTick(), we're really pausing before the reader is
      // connected.
      //
      reader.pause();
      reader.on('message', function (msg) {
        msg.finish();
        done(new Error('Should not have received a message while paused'));
      });

      writer.publish(topic, 'pause test');
    });

    it('should not receive any new messages when paused', function (done) {
      writer.publish(topic, { messageShouldArrive: true });

      reader.on('error', function (err) {
        console.log(err);
      });

      reader.on('message', function (msg) {
        // check the message
        msg.json().messageShouldArrive.should.be.true();
        msg.finish();

        if (reader.isPaused()) return done();

        reader.pause();

        process.nextTick(function () {
          // send it again, shouldn't get this one
          writer.publish(topic, { messageShouldArrive: false });
          setTimeout(done, 50);
        });
      });
    });

    it('should not receive any requeued messages when paused', function (done) {
      writer.publish(topic, 'requeue me');
      var id = '';

      reader.on('message', function (msg) {
        // this will fail if the msg comes through again
        id.should.equal('');
        id = msg.id;

        reader.pause();

        // send it again, shouldn't get this one
        msg.requeue(0, false);
        setTimeout(done, 50);
      });

      reader.on('error', function (err) {
        console.log(err);
      });
    });

    it('should start receiving messages again after unpause', function (done) {
      var paused = false;
      var handlerFn = null;
      var afterHandlerFn = null;

      var firstMessage = function firstMessage(msg) {
        reader.pause();
        paused = true;
        msg.requeue();
      };

      var secondMessage = function secondMessage(msg) {
        msg.finish();
      };

      reader.on('message', function (msg) {
        should.equal(paused, false);
        handlerFn(msg);

        if (afterHandlerFn) {
          afterHandlerFn();
        }
      });

      async.series([
      // Publish and handle first message
      function (callback) {
        handlerFn = firstMessage;
        afterHandlerFn = callback;

        writer.publish(topic, 'not paused', function (err) {
          if (err) {
            callback(err);
          }
        });
      },
      // Publish second message
      function (callback) {
        afterHandlerFn = callback;
        writer.publish(topic, 'paused', callback);
      },
      // Wait for 50ms
      function (callback) {
        setTimeout(callback, 50);
      },
      // Unpause. Processed queued message.
      function (callback) {
        handlerFn = secondMessage;
        // Note: We know a message was processed after unpausing when this
        // callback is called. No need to explicitly note a 2nd message was
        // processed.
        afterHandlerFn = callback;

        reader.unpause();
        paused = false;
      }], done);
    });

    it('should successfully publish a message before fully connected', function (done) {
      writer = new nsq.Writer('127.0.0.1', TCP_PORT);
      writer.connect();

      // The writer is connecting, but it shouldn't be ready to publish.
      should.equal(writer.ready, false);

      writer.on('error', function () {});

      // Publish the message. It should succeed since the writer will queue up
      // the message while connecting.
      writer.publish('a_topic', 'a message', function (err) {
        should.not.exist(err);
        done();
      });
    });
  });
});

describe('failures', function () {
  var nsqdProcess = null;

  before(function (done) {
    temp.mkdir('/nsq', function (err, dirPath) {
      if (err) return done(err);

      startNSQD(dirPath, {}, function (err, process) {
        nsqdProcess = process;
        done(err);
      });
    });
  });

  describe('Writer', function () {
    describe('nsqd disconnect before publish', function () {
      it('should fail to publish a message', function (done) {
        var writer = new nsq.Writer('127.0.0.1', TCP_PORT);
        async.series([
        // Connect the writer to the nsqd.
        function (callback) {
          writer.connect();
          writer.on('ready', callback);
          writer.on('error', function () {}); // Ensure error message is handled.
        },

        // Stop the nsqd process.
        function (callback) {
          nsqdProcess.on('exit', callback);
          nsqdProcess.kill('SIGKILL');
        },

        // Attempt to publish a message.
        function (callback) {
          writer.publish('test_topic', 'a message that should fail', function (err) {
            should.exist(err);
            callback();
          });
        }], done);
      });
    });
  });
});