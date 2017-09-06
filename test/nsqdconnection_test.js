'use strict';

var _ = require('lodash');
var should = require('should');
var sinon = require('sinon');

var wire = require('../src/wire');

var _require = require('../src/nsqdconnection'),
    ConnectionState = _require.ConnectionState,
    NSQDConnection = _require.NSQDConnection,
    WriterNSQDConnection = _require.WriterNSQDConnection,
    WriterConnectionState = _require.WriterConnectionState;

describe('Reader ConnectionState', function () {
  var state = {
    sent: [],
    connection: null,
    statemachine: null
  };

  beforeEach(function () {
    var sent = [];

    var connection = new NSQDConnection('127.0.0.1', 4150, 'topic_test', 'channel_test');
    sinon.stub(connection, 'write').callsFake(function (data) {
      return sent.push(data.toString());
    });
    sinon.stub(connection, 'close').callsFake(function () {});
    sinon.stub(connection, 'destroy').callsFake(function () {});

    var statemachine = new ConnectionState(connection);

    return _.extend(state, {
      sent: sent,
      connection: connection,
      statemachine: statemachine
    });
  });

  it('handle initial handshake', function () {
    var statemachine = state.statemachine,
        sent = state.sent;

    statemachine.raise('connecting');
    statemachine.raise('connected');
    sent[0].should.match(/^ {2}V2$/);
    sent[1].should.match(/^IDENTIFY/);
  });

  it('handle OK identify response', function () {
    var statemachine = state.statemachine,
        connection = state.connection;

    statemachine.raise('connecting');
    statemachine.raise('connected');
    statemachine.raise('response', new Buffer('OK'));

    should.equal(connection.maxRdyCount, 2500);
    should.equal(connection.maxMsgTimeout, 900000);
    should.equal(connection.msgTimeout, 60000);
  });

  it('handle identify response', function () {
    var statemachine = state.statemachine,
        connection = state.connection;

    statemachine.raise('connecting');
    statemachine.raise('connected');

    statemachine.raise('response', JSON.stringify({
      max_rdy_count: 1000,
      max_msg_timeout: 10 * 60 * 1000,
      msg_timeout: 2 * 60 * 1000
    }));

    should.equal(connection.maxRdyCount, 1000);
    should.equal(connection.maxMsgTimeout, 600000);
    should.equal(connection.msgTimeout, 120000);
  });

  it('create a subscription', function (done) {
    var sent = state.sent,
        statemachine = state.statemachine,
        connection = state.connection;

    // Subscribe notification

    connection.on(NSQDConnection.READY, function () {
      return done();
    });

    statemachine.raise('connecting');
    statemachine.raise('connected');
    statemachine.raise('response', 'OK'); // Identify response

    sent[2].should.match(/^SUB topic_test channel_test\n$/);
    statemachine.raise('response', 'OK');
  });

  it('handle a message', function (done) {
    var statemachine = state.statemachine,
        connection = state.connection;

    connection.on(NSQDConnection.MESSAGE, function () {
      return done();
    });

    statemachine.raise('connecting');
    statemachine.raise('connected');
    statemachine.raise('response', 'OK'); // Identify response
    statemachine.raise('response', 'OK'); // Subscribe response

    should.equal(statemachine.current_state_name, 'READY_RECV');

    statemachine.raise('consumeMessage', {});
    should.equal(statemachine.current_state_name, 'READY_RECV');
  });

  it('handle a message finish after a disconnect', function (done) {
    var statemachine = state.statemachine,
        connection = state.connection;

    sinon.stub(wire, 'unpackMessage').callsFake(function () {
      return ['1', 0, 0, new Buffer(''), 60, 60, 120];
    });

    connection.on(NSQDConnection.MESSAGE, function (msg) {
      var fin = function fin() {
        msg.finish();
        done();
      };
      setTimeout(fin, 10);
    });

    // Advance the connection to the READY state.
    statemachine.raise('connecting');
    statemachine.raise('connected');
    statemachine.raise('response', 'OK'); // Identify response
    statemachine.raise('response', 'OK'); // Subscribe response

    // Receive message
    var msg = connection.createMessage('');
    statemachine.raise('consumeMessage', msg);

    // Close the connection before the message has been processed.
    connection.destroy();
    statemachine.goto('CLOSED');

    // Undo stub
    wire.unpackMessage.restore();
  });

  it('handles non-fatal errors', function (done) {
    var connection = state.connection,
        statemachine = state.statemachine;

    // Note: we still want an error event raised, just not a closed connection

    connection.on(NSQDConnection.ERROR, function () {
      return done();
    });

    // Yields an error if the connection actually closes
    connection.on(NSQDConnection.CLOSED, function () {
      done(new Error('Should not have closed!'));
    });

    statemachine.goto('ERROR', new Error('E_REQ_FAILED'));
  });
});

describe('WriterConnectionState', function () {
  var state = {
    sent: [],
    connection: null,
    statemachine: null
  };

  beforeEach(function () {
    var sent = [];
    var connection = new WriterNSQDConnection('127.0.0.1', 4150);
    sinon.stub(connection, 'destroy');

    sinon.stub(connection, 'write').callsFake(function (data) {
      sent.push(data.toString());
    });

    var statemachine = new WriterConnectionState(connection);
    connection.statemachine = statemachine;

    _.extend(state, {
      sent: sent,
      connection: connection,
      statemachine: statemachine
    });
  });

  it('should generate a READY event after IDENTIFY', function (done) {
    var statemachine = state.statemachine,
        connection = state.connection;


    connection.on(WriterNSQDConnection.READY, function () {
      should.equal(statemachine.current_state_name, 'READY_SEND');
      done();
    });

    statemachine.raise('connecting');
    statemachine.raise('connected');
    statemachine.raise('response', 'OK');
  });

  it('should use PUB when sending a single message', function (done) {
    var statemachine = state.statemachine,
        connection = state.connection,
        sent = state.sent;


    connection.on(WriterNSQDConnection.READY, function () {
      connection.produceMessages('test', ['one']);
      sent[sent.length - 1].should.match(/^PUB/);
      done();
    });

    statemachine.raise('connecting');
    statemachine.raise('connected');
    statemachine.raise('response', 'OK');
  });

  it('should use MPUB when sending multiplie messages', function (done) {
    var statemachine = state.statemachine,
        connection = state.connection,
        sent = state.sent;


    connection.on(WriterNSQDConnection.READY, function () {
      connection.produceMessages('test', ['one', 'two']);
      sent[sent.length - 1].should.match(/^MPUB/);
      done();
    });

    statemachine.raise('connecting');
    statemachine.raise('connected');
    statemachine.raise('response', 'OK');
  });

  it('should call the callback when supplied on publishing a message', function (done) {
    var statemachine = state.statemachine,
        connection = state.connection;


    connection.on(WriterNSQDConnection.READY, function () {
      connection.produceMessages('test', ['one'], undefined, function () {
        return done();
      });
      statemachine.raise('response', 'OK');
    });

    statemachine.raise('connecting');
    statemachine.raise('connected');
    statemachine.raise('response', 'OK');
  });

  it('should call the the right callback on several messages', function (done) {
    var statemachine = state.statemachine,
        connection = state.connection;


    connection.on(WriterNSQDConnection.READY, function () {
      connection.produceMessages('test', ['one'], undefined);
      connection.produceMessages('test', ['two'], undefined, function () {
        // There should be no more callbacks
        should.equal(connection.messageCallbacks.length, 0);
        done();
      });

      statemachine.raise('response', 'OK');
      statemachine.raise('response', 'OK');
    });

    statemachine.raise('connecting');
    statemachine.raise('connected');
    statemachine.raise('response', 'OK');
  });

  it('should call all callbacks on nsqd disconnect', function (done) {
    var statemachine = state.statemachine,
        connection = state.connection;


    var firstCb = sinon.spy();
    var secondCb = sinon.spy();

    connection.on(WriterNSQDConnection.ERROR, function () {});

    connection.on(WriterNSQDConnection.READY, function () {
      connection.produceMessages('test', ['one'], undefined, firstCb);
      connection.produceMessages('test', ['two'], undefined, secondCb);
      statemachine.goto('ERROR', 'lost connection');
    });

    connection.on(WriterNSQDConnection.CLOSED, function () {
      firstCb.calledOnce.should.be.ok();
      secondCb.calledOnce.should.be.ok();
      done();
    });

    statemachine.raise('connecting');
    statemachine.raise('connected');
    statemachine.raise('response', 'OK');
  });
});