'use strict';

var _slicedToArray = function () { function sliceIterator(arr, i) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"]) _i["return"](); } finally { if (_d) throw _e; } } return _arr; } return function (arr, i) { if (Array.isArray(arr)) { return arr; } else if (Symbol.iterator in Object(arr)) { return sliceIterator(arr, i); } else { throw new TypeError("Invalid attempt to destructure non-iterable instance"); } }; }();

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _toConsumableArray(arr) { if (Array.isArray(arr)) { for (var i = 0, arr2 = Array(arr.length); i < arr.length; i++) { arr2[i] = arr[i]; } return arr2; } else { return Array.from(arr); } }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var _require = require('events'),
    EventEmitter = _require.EventEmitter;

var _ = require('lodash');
var should = require('should');
var sinon = require('sinon');

var Message = require('../src/message');

var _require2 = require('../src/nsqdconnection'),
    NSQDConnection = _require2.NSQDConnection;

var _require3 = require('../src/readerrdy'),
    ReaderRdy = _require3.ReaderRdy,
    ConnectionRdy = _require3.ConnectionRdy;

var StubNSQDConnection = function (_EventEmitter) {
  _inherits(StubNSQDConnection, _EventEmitter);

  function StubNSQDConnection(nsqdHost, nsqdPort, topic, channel, requeueDelay, heartbeatInterval) {
    _classCallCheck(this, StubNSQDConnection);

    var _this = _possibleConstructorReturn(this, (StubNSQDConnection.__proto__ || Object.getPrototypeOf(StubNSQDConnection)).call(this));

    _this.nsqdHost = nsqdHost;
    _this.nsqdPort = nsqdPort;
    _this.topic = topic;
    _this.channel = channel;
    _this.requeueDelay = requeueDelay;
    _this.heartbeatInterval = heartbeatInterval;
    _this.conn = { localPort: 1 };
    _this.maxRdyCount = 2500;
    _this.msgTimeout = 60 * 1000;
    _this.maxMsgTimeout = 15 * 60 * 1000;
    _this.rdyCounts = [];
    return _this;
  }

  _createClass(StubNSQDConnection, [{
    key: 'id',
    value: function id() {
      return this.nsqdHost + ':' + this.nsqdPort;
    }

    // Empty

  }, {
    key: 'connect',
    value: function connect() {}

    // Empty

  }, {
    key: 'close',
    value: function close() {}

    // Empty

  }, {
    key: 'destroy',
    value: function destroy() {}

    // Empty

  }, {
    key: 'setRdy',
    value: function setRdy(rdyCount) {
      this.rdyCounts.push(rdyCount);
    }
  }, {
    key: 'createMessage',
    value: function createMessage(msgId, msgTimestamp, attempts, msgBody) {
      var _this2 = this;

      var msgComponents = [msgId, msgTimestamp, attempts, msgBody];
      var msgArgs = msgComponents.concat([this.requeueDelay, this.msgTimeout, this.maxMsgTimeout]);
      var msg = new (Function.prototype.bind.apply(Message, [null].concat(_toConsumableArray(msgArgs))))();

      msg.on(Message.RESPOND, function (responseType) {
        if (responseType === Message.FINISH) {
          _this2.emit(NSQDConnection.FINISHED);
        } else if (responseType === Message.REQUEUE) {
          _this2.emit(NSQDConnection.REQUEUED);
        }
      });

      msg.on(Message.BACKOFF, function () {
        return _this2.emit(NSQDConnection.BACKOFF);
      });

      this.emit(NSQDConnection.MESSAGE, msg);
      return msg;
    }
  }]);

  return StubNSQDConnection;
}(EventEmitter);

var createNSQDConnection = function createNSQDConnection(id) {
  var conn = new StubNSQDConnection('host' + id, '4150', 'test', 'default', 60, 30);
  conn.conn.localPort = id;
  return conn;
};

describe('ConnectionRdy', function () {
  var _Array$from = Array.from([null, null, null]),
      _Array$from2 = _slicedToArray(_Array$from, 3),
      conn = _Array$from2[0],
      spy = _Array$from2[1],
      cRdy = _Array$from2[2];

  beforeEach(function () {
    conn = createNSQDConnection(1);
    spy = sinon.spy(conn, 'setRdy');
    cRdy = new ConnectionRdy(conn);
    cRdy.start();
  });

  it('should register listeners on a connection', function () {
    conn = new NSQDConnection('localhost', 1234, 'test', 'test');
    var mock = sinon.mock(conn);
    mock.expects('on').withArgs(NSQDConnection.ERROR);
    mock.expects('on').withArgs(NSQDConnection.FINISHED);
    mock.expects('on').withArgs(NSQDConnection.MESSAGE);
    mock.expects('on').withArgs(NSQDConnection.REQUEUED);
    mock.expects('on').withArgs(NSQDConnection.READY);
    cRdy = new ConnectionRdy(conn);
    mock.verify();
  });

  it('should have a connection RDY max of zero', function () {
    should.equal(cRdy.maxConnRdy, 0);
  });

  it('should not increase RDY when connection RDY max has not been set', function () {
    // This bump should be a no-op
    cRdy.bump();
    should.equal(cRdy.maxConnRdy, 0);
    should.equal(spy.called, false);
  });

  it('should not allow RDY counts to be negative', function () {
    cRdy.setConnectionRdyMax(10);
    cRdy.setRdy(-1);
    should.equal(spy.notCalled, true);
  });

  it('should not allow RDY counts to exceed the connection max', function () {
    cRdy.setConnectionRdyMax(10);
    cRdy.setRdy(9);
    cRdy.setRdy(10);
    cRdy.setRdy(20);
    should.equal(spy.calledTwice, true);
    should.equal(spy.firstCall.args[0], 9);
    should.equal(spy.secondCall.args[0], 10);
  });

  it('should set RDY to max after initial bump', function () {
    cRdy.setConnectionRdyMax(3);
    cRdy.bump();
    should.equal(spy.firstCall.args[0], 3);
  });

  it('should keep RDY at max after 1+ bumps', function () {
    cRdy.setConnectionRdyMax(3);
    for (var i = 1; i <= 3; i++) {
      cRdy.bump();
    }

    cRdy.maxConnRdy.should.eql(3);
    for (var _i = 0; _i < spy.callCount; _i++) {
      should.ok(spy.getCall(_i).args[0] <= 3);
    }
  });

  it('should set RDY to zero from after first bump and then backoff', function () {
    cRdy.setConnectionRdyMax(3);
    cRdy.bump();
    cRdy.backoff();
    should.equal(spy.lastCall.args[0], 0);
  });

  it('should set RDY to zero after 1+ bumps and then a backoff', function () {
    cRdy.setConnectionRdyMax(3);
    cRdy.bump();
    cRdy.backoff();
    should.equal(spy.lastCall.args[0], 0);
  });

  it('should raise RDY when new connection RDY max is lower', function () {
    cRdy.setConnectionRdyMax(3);
    cRdy.bump();
    cRdy.setConnectionRdyMax(5);
    should.equal(cRdy.maxConnRdy, 5);
    should.equal(spy.lastCall.args[0], 5);
  });

  it('should reduce RDY when new connection RDY max is higher', function () {
    cRdy.setConnectionRdyMax(3);
    cRdy.bump();
    cRdy.setConnectionRdyMax(2);
    should.equal(cRdy.maxConnRdy, 2);
    should.equal(spy.lastCall.args[0], 2);
  });

  it('should update RDY when 75% of previous RDY is consumed', function () {
    var msg = void 0;
    cRdy.setConnectionRdyMax(10);
    cRdy.bump();

    should.equal(spy.firstCall.args[0], 10);

    for (var i = 1; i <= 7; i++) {
      msg = conn.createMessage('' + i, Date.now(), 0, 'Message ' + i);
      msg.finish();
      cRdy.bump();
    }

    should.equal(spy.callCount, 1);

    msg = conn.createMessage('8', Date.now(), 0, 'Message 8');
    msg.finish();
    cRdy.bump();

    should.equal(spy.callCount, 2);
    should.equal(spy.lastCall.args[0], 10);
  });
});

describe('ReaderRdy', function () {
  var readerRdy = null;

  beforeEach(function () {
    readerRdy = new ReaderRdy(1, 128, 'topic/channel');
  });

  afterEach(function () {
    return readerRdy.close();
  });

  it('should register listeners on a connection', function () {
    // Stub out creation of ConnectionRdy to ignore the events registered by
    // ConnectionRdy.
    sinon.stub(readerRdy, 'createConnectionRdy').callsFake(function () {
      return {
        on: function on() {}
      };
    });
    // Empty

    var conn = createNSQDConnection(1);
    var mock = sinon.mock(conn);
    mock.expects('on').withArgs(NSQDConnection.CLOSED);
    mock.expects('on').withArgs(NSQDConnection.FINISHED);
    mock.expects('on').withArgs(NSQDConnection.REQUEUED);
    mock.expects('on').withArgs(NSQDConnection.BACKOFF);

    readerRdy.addConnection(conn);
    mock.verify();
  });

  it('should be in the zero state until a new connection is READY', function () {
    var conn = createNSQDConnection(1);
    readerRdy.current_state_name.should.eql('ZERO');
    readerRdy.addConnection(conn);
    readerRdy.current_state_name.should.eql('ZERO');
    conn.emit(NSQDConnection.READY);
    readerRdy.current_state_name.should.eql('MAX');
  });

  it('should be in the zero state if it loses all connections', function () {
    var conn = createNSQDConnection(1);
    readerRdy.addConnection(conn);
    conn.emit(NSQDConnection.READY);
    conn.emit(NSQDConnection.CLOSED);
    readerRdy.current_state_name.should.eql('ZERO');
  });

  it('should evenly distribute RDY count across connections', function () {
    readerRdy = new ReaderRdy(100, 128, 'topic/channel');

    var conn1 = createNSQDConnection(1);
    var conn2 = createNSQDConnection(2);

    var setRdyStub1 = sinon.spy(conn1, 'setRdy');
    var setRdyStub2 = sinon.spy(conn2, 'setRdy');

    readerRdy.addConnection(conn1);
    conn1.emit(NSQDConnection.READY);

    setRdyStub1.lastCall.args[0].should.eql(100);

    readerRdy.addConnection(conn2);
    conn2.emit(NSQDConnection.READY);

    setRdyStub1.lastCall.args[0].should.eql(50);
    setRdyStub2.lastCall.args[0].should.eql(50);
  });

  describe('low RDY conditions', function () {
    var assertAlternatingRdyCounts = function assertAlternatingRdyCounts(conn1, conn2) {
      var minSize = Math.min(conn1.rdyCounts.length, conn2.rdyCounts.length);

      var zippedCounts = _.zip(conn1.rdyCounts.slice(-minSize), conn2.rdyCounts.slice(-minSize));

      // We expect the connection RDY counts to look like this:
      // conn 0: [1, 0, 1, 0]
      // conn 1: [0, 1, 0, 1]
      zippedCounts.forEach(function (_ref) {
        var _ref2 = _slicedToArray(_ref, 2),
            firstRdy = _ref2[0],
            secondRdy = _ref2[1];

        should.ok(firstRdy + secondRdy === 1);
      });
    };

    it('should periodically redistribute RDY', function (done) {
      // Shortening the periodically `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01);

      var connections = [1, 2].map(function (i) {
        return createNSQDConnection(i);
      });

      // Add the connections and trigger the NSQDConnection event that tells
      // listeners that the connections are connected and ready for message flow.
      connections.forEach(function (conn) {
        readerRdy.addConnection(conn);
        conn.emit(NSQDConnection.READY);
      });

      // Given the number of connections and the maxInFlight, we should be in low
      // RDY conditions.
      should.equal(readerRdy.isLowRdy(), true);

      var checkRdyCounts = function checkRdyCounts() {
        assertAlternatingRdyCounts.apply(undefined, _toConsumableArray(connections));
        done();
      };

      // We have to wait a small period of time for log events to occur since the
      // `balance` call is invoked perdiocally.
      setTimeout(checkRdyCounts, 50);
    });

    it('should handle the transition from normal', function (done) {
      // Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01);

      var conn1 = createNSQDConnection(1);
      var conn2 = createNSQDConnection(2);

      // Add the connections and trigger the NSQDConnection event that tells
      // listeners that the connections are connected and ready for message flow.
      readerRdy.addConnection(conn1);
      conn1.emit(NSQDConnection.READY);

      should.equal(readerRdy.isLowRdy(), false);

      var addConnection = function addConnection() {
        readerRdy.addConnection(conn2);
        conn2.emit(NSQDConnection.READY);

        // Given the number of connections and the maxInFlight, we should be in
        // low RDY conditions.
        should.equal(readerRdy.isLowRdy(), true);
      };

      // Add the 2nd connections after some duration to simulate a new nsqd being
      // discovered and connected.
      setTimeout(addConnection, 20);

      var checkRdyCounts = function checkRdyCounts() {
        assertAlternatingRdyCounts(conn1, conn2);
        done();
      };

      // We have to wait a small period of time for log events to occur since the
      // `balance` call is invoked perdiocally.
      setTimeout(checkRdyCounts, 40);
    });

    it('should handle the transition to normal conditions', function (done) {
      // Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01);

      var connections = [1, 2].map(function (i) {
        return createNSQDConnection(i);
      });

      // Add the connections and trigger the NSQDConnection event that tells
      // listeners that the connections are connected and ready for message flow.
      connections.forEach(function (conn) {
        readerRdy.addConnection(conn);
        conn.emit(NSQDConnection.READY);
      });

      should.equal(readerRdy.isLowRdy(), true);
      readerRdy.isLowRdy().should.eql(true);

      var checkNormal = function checkNormal() {
        should.equal(readerRdy.isLowRdy(), false);
        should.equal(readerRdy.balanceId, null);
        should.equal(readerRdy.connections[0].lastRdySent, 1);
        done();
      };

      var removeConnection = function removeConnection() {
        connections[1].emit(NSQDConnection.CLOSED);
        setTimeout(checkNormal, 20);
      };

      // Remove a connection after some period of time to get back to normal
      // conditions.
      setTimeout(removeConnection, 20);
    });

    it('should move to normal conditions with connections in backoff', function (done) {
      /*
      1. Create two nsqd connections
      2. Close the 2nd connection when the first connection is in the BACKOFF
          state.
      3. Check to see if the 1st connection does get it's RDY count.
      */

      // Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01);

      var connections = [1, 2].map(function (i) {
        return createNSQDConnection(i);
      });

      connections.forEach(function (conn) {
        readerRdy.addConnection(conn);
        conn.emit(NSQDConnection.READY);
      });

      should.equal(readerRdy.isLowRdy(), true);

      var checkNormal = function checkNormal() {
        should.equal(readerRdy.isLowRdy(), false);
        should.equal(readerRdy.balanceId, null);
        should.equal(readerRdy.connections[0].lastRdySent, 1);
        done();
      };

      var removeConnection = _.once(function () {
        connections[1].emit(NSQDConnection.CLOSED);
        setTimeout(checkNormal, 30);
      });

      var removeOnBackoff = function removeOnBackoff() {
        var connRdy1 = readerRdy.connections[0];
        connRdy1.on(ConnectionRdy.STATE_CHANGE, function () {
          if (connRdy1.statemachine.current_state_name === 'BACKOFF') {
            // If we don't do the connection CLOSED in the next tick, we remove
            // the connection immediately which leaves `@connections` within
            // `balance` in an inconsistent state which isn't possible normally.
            setTimeout(removeConnection, 0);
          }
        });
      };

      // Remove a connection after some period of time to get back to normal
      // conditions.
      setTimeout(removeOnBackoff, 20);
    });

    it('should not exceed maxInFlight for long running message.', function (done) {
      // Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01);

      var connections = [1, 2].map(function (i) {
        return createNSQDConnection(i);
      });

      connections.forEach(function (conn) {
        readerRdy.addConnection(conn);
        conn.emit(NSQDConnection.READY);
      });

      // Handle the message but delay finishing the message so that several
      // balance calls happen and the check to ensure that RDY count is zero for
      // all connections.
      var handleMessage = function handleMessage(msg) {
        var finish = function finish() {
          msg.finish();
          done();
        };

        setTimeout(finish, 40);
      };

      connections.forEach(function (conn) {
        conn.on(NSQDConnection.MESSAGE, handleMessage);
      });

      // When the message is in-flight, balance cannot give a RDY count out to
      // any of the connections.
      var checkRdyCount = function checkRdyCount() {
        should.equal(readerRdy.isLowRdy(), true);
        should.equal(readerRdy.connections[0].lastRdySent, 0);
        should.equal(readerRdy.connections[1].lastRdySent, 0);
      };

      var sendMessageOnce = _.once(function () {
        connections[1].createMessage('1', Date.now(), new Buffer('test'));
        setTimeout(checkRdyCount, 20);
      });

      // Send a message on the 2nd connection when we can. Only send the message
      // once so that we don't violate the maxInFlight count.
      var sendOnRdy = function sendOnRdy() {
        var connRdy2 = readerRdy.connections[1];
        connRdy2.on(ConnectionRdy.STATE_CHANGE, function () {
          if (['ONE', 'MAX'].includes(connRdy2.statemachine.current_state_name)) {
            sendMessageOnce();
          }
        });
      };

      // We have to wait a small period of time for log events to occur since the
      // `balance` call is invoked perdiocally.
      setTimeout(sendOnRdy, 20);
    });

    it('should recover losing a connection with a message in-flight', function (done) {
      /*
      Detailed description:
      1. Connect to 5 nsqds and add them to the ReaderRdy
      2. When the 1st connection has the shared RDY count, it receives a
         message.
      3. On receipt of a message, the 1st connection will process the message
         for a long period of time.
      4. While the message is being processed, the 1st connection will close.
      5. Finally, check that the other connections are indeed now getting the
         RDY count.
      */

      // Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01);

      var connections = [1, 2, 3, 4, 5].map(function (i) {
        return createNSQDConnection(i);
      });

      // Add the connections and trigger the NSQDConnection event that tells
      // listeners that the connections are connected and ready for message flow.
      connections.forEach(function (conn) {
        readerRdy.addConnection(conn);
        conn.emit(NSQDConnection.READY);
      });

      var closeConnection = _.once(function () {
        connections[0].emit(NSQDConnection.CLOSED);
      });

      // When the message is in-flight, balance cannot give a RDY count out to
      // any of the connections.
      var checkRdyCount = function checkRdyCount() {
        should.equal(readerRdy.isLowRdy(), true);

        var rdyCounts = Array.from(readerRdy.connections).map(function (connRdy) {
          return connRdy.lastRdySent;
        });

        should.equal(readerRdy.connections.length, 4);
        should.ok(Array.from(rdyCounts).includes(1));
      };

      var handleMessage = function handleMessage(msg) {
        var delayFinish = function delayFinish() {
          msg.finish();
          done();
        };

        setTimeout(closeConnection, 10);
        setTimeout(checkRdyCount, 30);
        setTimeout(delayFinish, 50);
      };

      connections.forEach(function (conn) {
        conn.on(NSQDConnection.MESSAGE, handleMessage);
      });

      var sendMessageOnce = _.once(function () {
        connections[0].createMessage('1', Date.now(), new Buffer('test'));
      });

      // Send a message on the 2nd connection when we can. Only send the message
      // once so that we don't violate the maxInFlight count.
      var sendOnRdy = function sendOnRdy() {
        var connRdy = readerRdy.connections[0];
        connRdy.on(ConnectionRdy.STATE_CHANGE, function () {
          if (['ONE', 'MAX'].includes(connRdy.statemachine.current_state_name)) {
            sendMessageOnce();
          }
        });
      };

      // We have to wait a small period of time for log events to occur since the
      // `balance` call is invoked perdiocally.
      setTimeout(sendOnRdy, 10);
    });
  });

  describe('try', function () {
    it('should on completion of backoff attempt a single connection', function (done) {
      /*
      Detailed description:
      1. Create ReaderRdy with connections to 5 nsqds.
      2. Generate a message from an nsqd that causes a backoff.
      3. Verify that all the nsqds are in backoff mode.
      4. At the end of the backoff period, verify that only one ConnectionRdy
         is in the try one state and the others are still in backoff.
      */

      // Shortening the periodic `balance` calls to every 10ms. Changing the
      // max backoff duration to 10 sec.
      readerRdy = new ReaderRdy(100, 10, 'topic/channel', 0.01);

      var connections = [1, 2, 3, 4, 5].map(function (i) {
        return createNSQDConnection(i);
      });

      connections.forEach(function (conn) {
        readerRdy.addConnection(conn);
        conn.emit(NSQDConnection.READY);
      });

      connections[0].createMessage('1', Date.now(), 0, 'Message causing a backoff').requeue();

      var checkInBackoff = function checkInBackoff() {
        readerRdy.connections.forEach(function (connRdy) {
          connRdy.statemachine.current_state_name.should.eql('BACKOFF');
        });
      };

      checkInBackoff();

      var afterBackoff = function afterBackoff() {
        var states = readerRdy.connections.map(function (connRdy) {
          return connRdy.statemachine.current_state_name;
        });

        var ones = states.filter(function (state) {
          return state === 'ONE';
        });
        var backoffs = states.filter(function (state) {
          return state === 'BACKOFF';
        });

        should.equal(ones.length, 1);
        should.equal(backoffs.length, 4);
        done();
      };

      // Add 50ms to the delay so that we're confident that the event fired.
      var delay = readerRdy.backoffTimer.getInterval().plus(0.05);

      setTimeout(afterBackoff, delay.valueOf() * 1000);
    });

    it('should after backoff with a successful message go to MAX', function (done) {
      /*
      Detailed description:
      1. Create ReaderRdy with connections to 5 nsqds.
      2. Generate a message from an nsqd that causes a backoff.
      3. At the end of backoff, generate a message that will succeed.
      4. Verify that ReaderRdy is in MAX and ConnectionRdy instances are in
         either ONE or MAX. At least on ConnectionRdy should be in MAX as well.
      */

      // Shortening the periodica `balance` calls to every 10ms. Changing the
      // max backoff duration to 1 sec.
      readerRdy = new ReaderRdy(100, 1, 'topic/channel', 0.01);

      var connections = [1, 2, 3, 4, 5].map(function (i) {
        return createNSQDConnection(i);
      });

      connections.forEach(function (conn) {
        readerRdy.addConnection(conn);
        conn.emit(NSQDConnection.READY);
      });

      var msg = connections[0].createMessage('1', Date.now(), 0, 'Message causing a backoff');

      msg.requeue();

      var afterBackoff = function afterBackoff() {
        var _readerRdy$connection = readerRdy.connections.filter(function (conn) {
          return conn.statemachine.current_state_name === 'ONE';
        }),
            _readerRdy$connection2 = _slicedToArray(_readerRdy$connection, 1),
            connRdy = _readerRdy$connection2[0];

        msg = connRdy.conn.createMessage('1', Date.now(), 0, 'Success');
        msg.finish();

        var verifyMax = function verifyMax() {
          var states = readerRdy.connections.map(function (conn) {
            return conn.statemachine.current_state_name;
          });

          var max = states.filter(function (s) {
            return ['ONE', 'MAX'].includes(s);
          });

          max.length.should.eql(5);
          should.equal(max.length, 5);
          should.ok(states.includes('MAX'));
          done();
        };

        setTimeout(verifyMax, 0);
      };

      var delay = readerRdy.backoffTimer.getInterval() + 100;
      setTimeout(afterBackoff, delay * 1000);
    });
  });

  describe('backoff', function () {
    it('should not increase interval with more failures during backoff', function () {
      readerRdy = new ReaderRdy(100, 1, 'topic/channel', 0.01);

      // Create a connection and make it ready.
      var c = createNSQDConnection(0);
      readerRdy.addConnection(c);
      c.emit(NSQDConnection.READY);

      readerRdy.raise('backoff');
      var interval = readerRdy.backoffTimer.getInterval();

      readerRdy.raise('backoff');
      readerRdy.backoffTimer.getInterval().should.eql(interval);
    });
  });

  describe('pause / unpause', function () {
    beforeEach(function () {
      // Shortening the periodic `balance` calls to every 10ms. Changing the
      // max backoff duration to 1 sec.
      readerRdy = new ReaderRdy(100, 1, 'topic/channel', 0.01);

      var connections = [1, 2, 3, 4, 5].map(function (i) {
        return createNSQDConnection(i);
      });

      connections.forEach(function (conn) {
        readerRdy.addConnection(conn);
        conn.emit(NSQDConnection.READY);
      });
    });

    it('should drop ready count to zero on all connections when paused', function () {
      readerRdy.pause();
      should.equal(readerRdy.current_state_name, 'PAUSE');
      readerRdy.connections.forEach(function (conn) {
        return should.equal(conn.lastRdySent, 0);
      });
    });

    it('should unpause by trying one', function () {
      readerRdy.pause();
      readerRdy.unpause();
      should.equal(readerRdy.current_state_name, 'TRY_ONE');
    });

    it('should update the value of @isPaused when paused', function () {
      readerRdy.pause();
      should.equal(readerRdy.isPaused(), true);
      readerRdy.unpause();
      should.equal(readerRdy.isPaused(), false);
    });
  });
});