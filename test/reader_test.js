'use strict';

var should = require('should');
var sinon = require('sinon');

var nsq = require('../src/nsq');

describe('reader', function () {
  var readerWithAttempts = function readerWithAttempts(attempts) {
    return new nsq.Reader('topic', 'default', {
      nsqdTCPAddresses: ['127.0.0.1:4150'],
      maxAttempts: attempts
    });
  };

  describe('max attempts', function () {
    return describe('exceeded', function () {
      it('should finish after exceeding specified max attempts', function (done) {
        var maxAttempts = 2;
        var reader = readerWithAttempts(maxAttempts);

        // Message that has exceed the maximum number of attempts
        var message = {
          attempts: maxAttempts,
          finish: sinon.spy()
        };

        reader.handleMessage(message);

        process.nextTick(function () {
          should.equal(message.finish.called, true);
          done();
        });
      });

      it('should call the DISCARD message hanlder if registered', function (done) {
        var maxAttempts = 2;
        var reader = readerWithAttempts(maxAttempts);

        var message = {
          attempts: maxAttempts,
          finish: function finish() {}
        };

        reader.on(nsq.Reader.DISCARD, function () {
          return done();
        });
        reader.handleMessage(message);
      });

      it('should call the MESSAGE handler by default', function (done) {
        var maxAttempts = 2;
        var reader = readerWithAttempts(maxAttempts);

        var message = {
          attempts: maxAttempts,
          finish: function finish() {}
        };

        reader.on(nsq.Reader.MESSAGE, function () {
          return done();
        });
        reader.handleMessage(message);
      });
    });
  });

  describe('off by default', function () {
    return it('should not finish the message', function (done) {
      var reader = readerWithAttempts(0);

      var message = {
        attempts: 100,
        finish: sinon.spy()

        // Registering this to make sure that even if the listener is available,
        // it should not be getting called.
      };reader.on(nsq.Reader.DISCARD, function () {
        done(new Error('Unexpected discard message'));
      });

      var messageHandlerSpy = sinon.spy();
      reader.on(nsq.Reader.MESSAGE, messageHandlerSpy);
      reader.handleMessage(message);

      process.nextTick(function () {
        should.equal(messageHandlerSpy.called, true);
        should.equal(message.finish.called, false);
        done();
      });
    });
  });
});