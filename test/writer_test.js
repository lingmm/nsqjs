'use strict';

var should = require('should');
var sinon = require('sinon');

var nsq = require('../src/nsq');

describe('writer', function () {
  var writer = null;

  beforeEach(function () {
    writer = new nsq.Writer('127.0.0.1', '4150');
    writer.conn = { produceMessages: sinon.stub() };
  });

  afterEach(function () {
    writer = null;
  });

  describe('publish', function () {
    it('should publish a string', function () {
      var topic = 'test_topic';
      var msg = 'hello world!';

      writer.publish(topic, msg, function () {
        should.equal(writer.conn.produceMessages.calledOnce, true);
        should.equal(writer.conn.produceMessages.calledWith(topic, [msg]), true);
      });
    });

    it('should defer publish a string', function () {
      var topic = 'test_topic';
      var msg = 'hello world!';

      writer.publish(topic, msg, 300, function () {
        should.equal(writer.conn.produceMessages.calledOnce, true);
        should.equal(writer.conn.produceMessages.calledWith(topic, [msg]), true);
      });
    });

    // Add test where it is not ready yet

    it('should publish a list of strings', function () {
      var topic = 'test_topic';
      var msgs = ['hello world!', 'another message'];

      writer.publish(topic, msgs, function () {
        should.equal(writer.conn.produceMessages.calledOnce, true);
        should.equal(writer.conn.produceMessages.calledWith(topic, msgs), true);
      });
    });

    it('should publish a buffer', function () {
      var topic = 'test_topic';
      var msg = new Buffer('a buffer message');

      writer.publish(topic, msg, function () {
        should.equal(writer.conn.produceMessages.calledOnce, true);
        should.equal(writer.conn.produceMessages.calledWith(topic, [msg]), true);
      });
    });

    it('should publish an object as JSON', function () {
      var topic = 'test_topic';
      var msg = { a: 1 };

      writer.publish(topic, msg, function () {
        should.equal(writer.conn.produceMessages.calledOnce, true);
        should.equal(writer.conn.produceMessages.calledWith(topic, [JSON.stringify(msg)]), true);
      });
    });

    it('should publish a list of buffers', function () {
      var topic = 'test_topic';
      var msgs = [new Buffer('a buffer message'), new Buffer('another msg')];

      writer.publish(topic, msgs, function () {
        should.equal(writer.conn.produceMessages.calledOnce, true);
        should.equal(writer.conn.produceMessages.calledWith(topic, msgs), true);
      });
    });

    it('should publish a list of objects as JSON', function () {
      var topic = 'test_topic';
      var msgs = [{ a: 1 }, { b: 2 }];
      var encodedMsgs = Array.from(msgs).map(function (i) {
        return JSON.stringify(i);
      });

      writer.publish(topic, msgs, function () {
        should.equal(writer.conn.produceMessages.calledOnce, true);
        should.equal(writer.conn.produceMessages.calledWith(topic, encodedMsgs), true);
      });
    });

    it('should fail when publishing Null', function (done) {
      var topic = 'test_topic';
      var msg = null;

      writer.publish(topic, msg, function (err) {
        should.exist(err);
        done();
      });
    });

    it('should fail when publishing Undefined', function (done) {
      var topic = 'test_topic';
      var msg = undefined;

      writer.publish(topic, msg, function (err) {
        should.exist(err);
        done();
      });
    });

    it('should fail when publishing an empty string', function (done) {
      var topic = 'test_topic';
      var msg = '';

      writer.publish(topic, msg, function (err) {
        should.exist(err);
        done();
      });
    });

    it('should fail when publishing an empty list', function (done) {
      var topic = 'test_topic';
      var msg = [];

      writer.publish(topic, msg, function (err) {
        should.exist(err);
        done();
      });
    });

    it('should fail when the Writer is not connected', function (done) {
      writer = new nsq.Writer('127.0.0.1', '4150');
      writer.publish('test_topic', 'a briliant message', function (err) {
        should.exist(err);
        done();
      });
    });
  });
});