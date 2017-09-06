'use strict';

var should = require('should');
var sinon = require('sinon');
var Message = require('../src/message');

var createMessage = function createMessage(body, requeueDelay, timeout, maxTimeout) {
  return new Message('1', Date.now(), 0, new Buffer(body), requeueDelay, timeout, maxTimeout);
};

describe('Message', function () {
  return describe('timeout', function () {
    it('should not allow finishing a message twice', function (done) {
      var msg = createMessage('body', 90, 50, 100);

      var firstFinish = function firstFinish() {
        return msg.finish();
      };
      var secondFinish = function secondFinish() {
        msg.hasResponded.should.eql(true);
        done();
      };

      setTimeout(firstFinish, 10);
      setTimeout(secondFinish, 20);
    });

    it('should not allow requeue after finish', function (done) {
      var msg = createMessage('body', 90, 50, 100);

      var responseSpy = sinon.spy();
      msg.on(Message.RESPOND, responseSpy);

      var firstFinish = function firstFinish() {
        return msg.finish();
      };
      var secondRequeue = function secondRequeue() {
        return msg.requeue();
      };

      var check = function check() {
        responseSpy.calledOnce.should.be.true();
        done();
      };

      setTimeout(firstFinish, 10);
      setTimeout(secondRequeue, 20);
      setTimeout(check, 20);
    });

    it('should allow touch and then finish post first timeout', function (done) {
      var touchIn = 15;
      var timeoutIn = 20;
      var finishIn = 25;
      var checkIn = 30;

      var msg = createMessage('body', 90, timeoutIn, 100);
      var responseSpy = sinon.spy();
      msg.on(Message.RESPOND, responseSpy);

      var touch = function touch() {
        return msg.touch();
      };

      var finish = function finish() {
        msg.timedOut.should.be.eql(false);
        return msg.finish();
      };

      var check = function check() {
        responseSpy.calledTwice.should.be.true();
        return done();
      };

      setTimeout(touch, touchIn);
      setTimeout(finish, finishIn);
      return setTimeout(check, checkIn);
    });

    return it('should clear timeout on finish', function (done) {
      var msg = createMessage('body', 10, 60, 120);
      msg.finish();

      return process.nextTick(function () {
        should.not.exist(msg.trackTimeoutId);
        return done();
      });
    });
  });
});