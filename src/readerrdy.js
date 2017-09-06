'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var _require = require('events'),
    EventEmitter = _require.EventEmitter;

var NodeState = require('node-state');
var _ = require('lodash');
var debug = require('debug');

var BackoffTimer = require('./backofftimer');
var RoundRobinList = require('./roundrobinlist');

var _require2 = require('./nsqdconnection'),
    NSQDConnection = _require2.NSQDConnection;

/**
 * Maintains the RDY and in-flight counts for a nsqd connection. ConnectionRdy
 * ensures that the RDY count will not exceed the max set for this connection.
 * The max for the connection can be adjusted at any time.
 *
 * Usage:
 *   const connRdy = ConnectionRdy(conn);
 *   const connRdy.setConnectionRdyMax(10);
 *
 *   // On a successful message, bump up the RDY count for this connection.
 *   conn.on('message', () => connRdy.raise('bump'));
 *
 *   // We're backing off when we encounter a requeue. Wait 5 seconds to try
 *   // again.
 *   conn.on('requeue', () => connRdy.raise('backoff'));
 *   setTimeout(() => connRdy.raise (bump'), 5000);
*/


var ConnectionRdy = function (_EventEmitter) {
  _inherits(ConnectionRdy, _EventEmitter);

  _createClass(ConnectionRdy, null, [{
    key: 'READY',

    // Events emitted by ConnectionRdy
    get: function get() {
      return 'ready';
    }
  }, {
    key: 'STATE_CHANGE',
    get: function get() {
      return 'statechange';
    }

    /**
     * Instantiates a new ConnectionRdy event emitter.
     *
     * @param  {Object} conn
     * @constructor
     */

  }]);

  function ConnectionRdy(conn) {
    var _ref;

    _classCallCheck(this, ConnectionRdy);

    for (var _len = arguments.length, args = Array(_len > 1 ? _len - 1 : 0), _key = 1; _key < _len; _key++) {
      args[_key - 1] = arguments[_key];
    }

    var _this = _possibleConstructorReturn(this, (_ref = ConnectionRdy.__proto__ || Object.getPrototypeOf(ConnectionRdy)).call.apply(_ref, [this, conn].concat(args)));

    _this.conn = conn;
    var readerId = _this.conn.topic + '/' + _this.conn.channel;
    var connId = '' + _this.conn.id().replace(':', '/');
    _this.debug = debug('nsqjs:reader:' + readerId + ':rdy:conn:' + connId);

    _this.maxConnRdy = 0; // The absolutely maximum the RDY count can be per conn.
    _this.inFlight = 0; // The num. messages currently in-flight for this conn.
    _this.lastRdySent = 0; // The RDY value last sent to the server.
    _this.availableRdy = 0; // The RDY count remaining on the server for this conn.
    _this.statemachine = new ConnectionRdyState(_this);

    _this.conn.on(NSQDConnection.ERROR, function (err) {
      return _this.log(err);
    });
    _this.conn.on(NSQDConnection.MESSAGE, function () {
      if (_this.idleId != null) {
        clearTimeout(_this.idleId);
      }
      _this.idleId = null;
      _this.inFlight += 1;
      _this.availableRdy -= 1;
    });
    _this.conn.on(NSQDConnection.FINISHED, function () {
      return _this.inFlight--;
    });
    _this.conn.on(NSQDConnection.REQUEUED, function () {
      return _this.inFlight--;
    });
    _this.conn.on(NSQDConnection.READY, function () {
      return _this.start();
    });
    return _this;
  }

  /**
   * Close the reader ready connection.
   */


  _createClass(ConnectionRdy, [{
    key: 'close',
    value: function close() {
      this.conn.close();
    }

    /**
     * Return the name of the local port connection.
     *
     * @return {String}
     */

  }, {
    key: 'name',
    value: function name() {
      return String(this.conn.conn.localPort);
    }

    /**
     * Emit that the connection is ready.
     *
     * @return {Boolean} Returns true if the event had listeners, false otherwise.
     */

  }, {
    key: 'start',
    value: function start() {
      this.statemachine.start();
      return this.emit(ConnectionRdy.READY);
    }

    /**
     * Initialize the max number of connections ready.
     *
     * @param {Number} maxConnRdy
     */

  }, {
    key: 'setConnectionRdyMax',
    value: function setConnectionRdyMax(maxConnRdy) {
      this.log('setConnectionRdyMax ' + maxConnRdy);
      // The RDY count for this connection should not exceed the max RDY count
      // configured for this nsqd connection.
      this.maxConnRdy = Math.min(maxConnRdy, this.conn.maxRdyCount);
      this.statemachine.raise('adjustMax');
    }

    /**
     * Raises a `BUMP` event.
     */

  }, {
    key: 'bump',
    value: function bump() {
      this.statemachine.raise('bump');
    }

    /**
     * Raises a `BACKOFF` event.
     */

  }, {
    key: 'backoff',
    value: function backoff() {
      this.statemachine.raise('backoff');
    }

    /**
     * Used to identify when buffered messages should be processed
     * and responded to.
     *
     * @return {Boolean} [description]
     */

  }, {
    key: 'isStarved',
    value: function isStarved() {
      if (!(this.inFlight <= this.maxConnRdy)) {
        throw new Error('isStarved check is failing');
      }
      return this.inFlight === this.lastRdySent;
    }

    /**
     * Assign the number of readers available.
     *
     * @param {Number} rdyCount
     */

  }, {
    key: 'setRdy',
    value: function setRdy(rdyCount) {
      this.log('RDY ' + rdyCount);
      if (rdyCount < 0 || rdyCount > this.maxConnRdy) return;

      this.conn.setRdy(rdyCount);
      this.availableRdy = this.lastRdySent = rdyCount;
    }

    /**
     * @param  {String} message
     * @return {String}
     */

  }, {
    key: 'log',
    value: function log(message) {
      if (message) return this.debug(message);
    }
  }]);

  return ConnectionRdy;
}(EventEmitter);

/**
 * Internal statemachine used handle the various reader ready states.
 * @type {NodeState}
 */


var ConnectionRdyState = function (_NodeState) {
  _inherits(ConnectionRdyState, _NodeState);

  /**
   * Instantiates a new ConnectionRdyState.
   *
   * @param  {Object} connRdy reader connection
   * @constructor
   */
  function ConnectionRdyState(connRdy) {
    _classCallCheck(this, ConnectionRdyState);

    var _this2 = _possibleConstructorReturn(this, (ConnectionRdyState.__proto__ || Object.getPrototypeOf(ConnectionRdyState)).call(this, {
      autostart: false,
      initial_state: 'INIT',
      sync_goto: true
    }));

    _this2.connRdy = connRdy;
    return _this2;
  }

  /**
   * Utility function to log a message through debug.
   *
   * @param  {Message} message
   * @return {String}
   */


  _createClass(ConnectionRdyState, [{
    key: 'log',
    value: function log(message) {
      this.connRdy.debug(this.current_state_name);
      if (message) {
        return this.connRdy.debug(message);
      }
    }
  }]);

  return ConnectionRdyState;
}(NodeState);

ConnectionRdyState.prototype.states = {
  INIT: {
    // RDY is implicitly zero
    bump: function bump() {
      if (this.connRdy.maxConnRdy > 0) {
        return this.goto('MAX');
      }
    },
    backoff: function backoff() {},
    // No-op
    adjustMax: function adjustMax() {}
  }, // No-op

  BACKOFF: {
    Enter: function Enter() {
      return this.connRdy.setRdy(0);
    },
    bump: function bump() {
      if (this.connRdy.maxConnRdy > 0) return this.goto('ONE');
    },
    backoff: function backoff() {},
    // No-op
    adjustMax: function adjustMax() {}
  }, // No-op

  ONE: {
    Enter: function Enter() {
      return this.connRdy.setRdy(1);
    },
    bump: function bump() {
      return this.goto('MAX');
    },
    backoff: function backoff() {
      return this.goto('BACKOFF');
    },
    adjustMax: function adjustMax() {}
  }, // No-op

  MAX: {
    Enter: function Enter() {
      return this.connRdy.setRdy(this.connRdy.maxConnRdy);
    },
    bump: function bump() {
      // No need to keep setting the RDY count for versions of NSQD >= 0.3.0.
      var version = this.connRdy.conn != null ? this.connRdy.conn.nsqdVersion : undefined;
      if (!version || version.split('.') < [0, 3, 0]) {
        if (this.connRdy.availableRdy <= this.connRdy.lastRdySent * 0.25) {
          return this.connRdy.setRdy(this.connRdy.maxConnRdy);
        }
      }
    },
    backoff: function backoff() {
      return this.goto('BACKOFF');
    },
    adjustMax: function adjustMax() {
      this.log('adjustMax RDY ' + this.connRdy.maxConnRdy);
      return this.connRdy.setRdy(this.connRdy.maxConnRdy);
    }
  }
};

ConnectionRdyState.prototype.transitions = {
  '*': {
    '*': function _(data, callback) {
      this.log();
      callback(data);
      return this.connRdy.emit(ConnectionRdy.STATE_CHANGE);
    }
  }

  /**
   * Usage:
   *   const backoffTime = 90;
   *   const heartbeat = 30;
   *
   *   const [topic, channel] = ['sample', 'default'];
   *   const [host1, port1] = ['127.0.0.1', '4150'];
   *   const c1 = new NSQDConnection(host1, port1, topic, channel,
   *     backoffTime, heartbeat);
   *
   *   const readerRdy = new ReaderRdy(1, 128);
   *   readerRdy.addConnection(c1);
   *
   *   const message = (msg) => {
   *     console.log(`Callback [message]: ${msg.attempts}, ${msg.body.toString()}1);
   *     if (msg.attempts >= 5) {
   *       msg.finish();
   *       return;
   *     }
   *
   *     if (msg.body.toString() === 'requeue')
   *       msg.requeue();
   *     else
   *       msg.finish();
   *   }
   *
   *   const discard = (msg) => {
   *     console.log(`Giving up on this message: ${msg.id}`);
   *     msg.finish();
   *   }
   *
   *   c1.on(NSQDConnection.MESSAGE, message);
   *   c1.connect();
   */
};var READER_COUNT = 0;

/**
 * ReaderRdy statemachine.
 * @type {[type]}
 */

var ReaderRdy = function (_NodeState2) {
  _inherits(ReaderRdy, _NodeState2);

  _createClass(ReaderRdy, null, [{
    key: 'getId',

    /**
     * Generates a new ID for a reader connection.
     *
     * @return {Number}
     */
    value: function getId() {
      return READER_COUNT++;
    }

    /**
      * @constructor
      * @param  {Number} maxInFlight Maximum number of messages in-flight
      *   across all connections.
      * @param  {Number} maxBackoffDuration  The longest amount of time (secs)
      *   for a backoff event.
      * @param  {Number} readerId            The descriptive id for the Reader
      * @param  {Number} [lowRdyTimeout=1.5] Time (secs) to rebalane RDY count
      *   among connections
      */

  }]);

  function ReaderRdy(maxInFlight, maxBackoffDuration, readerId) {
    var lowRdyTimeout = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : 1.5;

    _classCallCheck(this, ReaderRdy);

    var _this3 = _possibleConstructorReturn(this, (ReaderRdy.__proto__ || Object.getPrototypeOf(ReaderRdy)).call(this, {
      autostart: true,
      initial_state: 'ZERO',
      sync_goto: true
    }));

    _this3.maxInFlight = maxInFlight;
    _this3.maxBackoffDuration = maxBackoffDuration;
    _this3.readerId = readerId;
    _this3.lowRdyTimeout = lowRdyTimeout;
    _this3.debug = debug('nsqjs:reader:' + _this3.readerId + ':rdy');

    _this3.id = ReaderRdy.getId();
    _this3.backoffTimer = new BackoffTimer(0, _this3.maxBackoffDuration);
    _this3.backoffId = null;
    _this3.balanceId = null;
    _this3.connections = [];
    _this3.roundRobinConnections = new RoundRobinList([]);
    return _this3;
  }

  /**
   * Close all reader connections.
   *
   * @return {Array} The closed connections.
   */


  _createClass(ReaderRdy, [{
    key: 'close',
    value: function close() {
      clearTimeout(this.backoffId);
      clearTimeout(this.balanceId);
      return this.connections.map(function (conn) {
        return conn.close();
      });
    }

    /**
     * Raise a `PAUSE` event.
     */

  }, {
    key: 'pause',
    value: function pause() {
      this.raise('pause');
    }

    /**
     * Raise a `UNPAUSE` event.
     */

  }, {
    key: 'unpause',
    value: function unpause() {
      this.raise('unpause');
    }

    /**
     * Indicates if a the reader ready connection has been paused.
     *
     * @return {Boolean}
     */

  }, {
    key: 'isPaused',
    value: function isPaused() {
      return this.current_state_name === 'PAUSE';
    }

    /**
     * @param  {String} message
     * @return {String}
     */

  }, {
    key: 'log',
    value: function log(message) {
      if (this.debug) {
        this.debug(this.current_state_name);

        if (message) return this.debug(message);
      }
    }

    /**
     * Used to identify when buffered messages should be processed
     * and responded to.
     *
     * @return {Boolean} [description]
     */

  }, {
    key: 'isStarved',
    value: function isStarved() {
      if (_.isEmpty(this.connections)) return false;

      return this.connections.filter(function (conn) {
        return conn.isStarved();
      }).length > 0;
    }

    /**
     * Creates a new ConnectionRdy statemachine.
     * @param  {Object} conn
     * @return {ConnectionRdy}
     */

  }, {
    key: 'createConnectionRdy',
    value: function createConnectionRdy(conn) {
      return new ConnectionRdy(conn);
    }

    /**
     * Indicates if a producer is in a state where RDY counts are re-distributed.
     * @return {Boolean}
     */

  }, {
    key: 'isLowRdy',
    value: function isLowRdy() {
      return this.maxInFlight < this.connections.length;
    }

    /**
     * Message success handler.
     *
     * @param  {ConnectionRdy} connectionRdy
     */

  }, {
    key: 'onMessageSuccess',
    value: function onMessageSuccess(connectionRdy) {
      if (!this.isPaused()) {
        if (this.isLowRdy()) {
          // Balance the RDY count amoung existing connections given the
          // low RDY condition.
          this.balance();
        } else {
          // Restore RDY count for connection to the connection max.
          connectionRdy.bump();
        }
      }
    }

    /**
     * Add a new connection to the pool.
     *
     * @param {Object} conn
     */

  }, {
    key: 'addConnection',
    value: function addConnection(conn) {
      var _this4 = this;

      var connectionRdy = this.createConnectionRdy(conn);

      conn.on(NSQDConnection.CLOSED, function () {
        _this4.removeConnection(connectionRdy);
        _this4.balance();
      });

      conn.on(NSQDConnection.FINISHED, function () {
        return _this4.raise('success', connectionRdy);
      });

      conn.on(NSQDConnection.REQUEUED, function () {
        // Since there isn't a guaranteed order for the REQUEUED and BACKOFF
        // events, handle the case when we handle BACKOFF and then REQUEUED.
        if (_this4.current_state_name !== 'BACKOFF' && !_this4.isPaused()) {
          connectionRdy.bump();
        }
      });

      conn.on(NSQDConnection.BACKOFF, function () {
        return _this4.raise('backoff');
      });

      connectionRdy.on(ConnectionRdy.READY, function () {
        _this4.connections.push(connectionRdy);
        _this4.roundRobinConnections.add(connectionRdy);

        _this4.balance();
        if (_this4.current_state_name === 'ZERO') {
          _this4.goto('MAX');
        } else if (['TRY_ONE', 'MAX'].includes(_this4.current_state_name)) {
          connectionRdy.bump();
        }
      });
    }

    /**
     * Remove a connection from the pool.
     *
     * @param  {Object} conn
     */

  }, {
    key: 'removeConnection',
    value: function removeConnection(conn) {
      this.connections.splice(this.connections.indexOf(conn), 1);
      this.roundRobinConnections.remove(conn);

      if (this.connections.length === 0) {
        this.goto('ZERO');
      }
    }

    /**
     * Raise a `BUMP` event for each connection in the pool.
     *
     * @return {Array} The bumped connections
     */

  }, {
    key: 'bump',
    value: function bump() {
      return this.connections.map(function (conn) {
        return conn.bump();
      });
    }

    /**
     * Try to balance the connection pool.
     */

  }, {
    key: 'try',
    value: function _try() {
      this.balance();
    }

    /**
     * Raise a `BACKOFF` event for each connection in the pool.
     */

  }, {
    key: 'backoff',
    value: function backoff() {
      var _this5 = this;

      this.connections.forEach(function (conn) {
        return conn.backoff();
      });

      if (this.backoffId) {
        clearTimeout(this.backoffId);
      }

      var onTimeout = function onTimeout() {
        _this5.log('Backoff done');
        _this5.raise('try');
      };

      // Convert from the BigNumber representation to Number.
      var delay = Number(this.backoffTimer.getInterval().valueOf()) * 1000;
      this.backoffId = setTimeout(onTimeout, delay);
      this.log('Backoff for ' + delay);
    }

    /**
     * Return the number of connections inflight.
     *
     * @return {Number}
     */

  }, {
    key: 'inFlight',
    value: function inFlight() {
      var add = function add(previous, conn) {
        return previous + conn.inFlight;
      };
      return this.connections.reduce(add, 0);
    }

    /**
     * The max connections readily available.
     *
     * @return {Number}
     */

  }, {
    key: 'maxConnectionsRdy',
    value: function maxConnectionsRdy() {
      switch (this.current_state_name) {
        case 'TRY_ONE':
          return 1;
        case 'PAUSE':
          return 0;
        default:
          return this.maxInFlight;
      }
    }

    /**
     * Evenly or fairly distributes RDY count based on the maxInFlight across
     * all nsqd connections.
     *
     * In the perverse situation where there are more connections than max in
     * flight, we do the following:
     *
     * There is a sliding window where each of the connections gets a RDY count
     * of 1. When the connection has processed it's single message, then
     * the RDY count is distributed to the next waiting connection. If
     * the connection does nothing with it's RDY count, then it should
     * timeout and give it's RDY count to another connection.
     */

  }, {
    key: 'balance',
    value: function balance() {
      var _this6 = this;

      this.log('balance');

      if (this.balanceId != null) {
        clearTimeout(this.balanceId);
        this.balanceId = null;
      }

      var max = this.maxConnectionsRdy();
      var perConnectionMax = Math.floor(max / this.connections.length);

      // Low RDY and try conditions
      if (perConnectionMax === 0) {
        /**
         * Backoff on all connections. In-flight messages from
         * connections will still be processed.
         */
        this.connections.forEach(function (conn) {
          return conn.backoff();
        });

        // Distribute available RDY count to the connections next in line.
        this.roundRobinConnections.next(max - this.inFlight()).forEach(function (conn) {
          conn.setConnectionRdyMax(1);
          conn.bump();
        });

        // Rebalance periodically. Needed when no messages are received.
        this.balanceId = setTimeout(function () {
          _this6.balance();
        }, this.lowRdyTimeout * 1000);
      } else {
        var rdyRemainder = this.maxInFlight % this.connectionsLength;
        this.connections.forEach(function (c) {
          var connMax = perConnectionMax;

          /**
           * Distribute the remainder RDY count evenly between the first
           * n connections.
           */
          if (rdyRemainder > 0) {
            connMax += 1;
            rdyRemainder -= 1;
          }

          c.setConnectionRdyMax(connMax);
          c.bump();
        });
      }
    }
  }]);

  return ReaderRdy;
}(NodeState);

/**
 * The following events results in transitions in the ReaderRdy state machine:
 * 1. Adding the first connection
 * 2. Remove the last connections
 * 3. Finish event from message handling
 * 4. Backoff event from message handling
 * 5. Backoff timeout
 */


ReaderRdy.prototype.states = {
  ZERO: {
    Enter: function Enter() {
      if (this.backoffId) {
        return clearTimeout(this.backoffId);
      }
    },
    backoff: function backoff() {},
    // No-op
    success: function success() {},
    // No-op
    try: function _try() {},
    // No-op
    pause: function pause() {
      // No-op
      return this.goto('PAUSE');
    },
    unpause: function unpause() {}
  }, // No-op

  PAUSE: {
    Enter: function Enter() {
      return this.connections.map(function (conn) {
        return conn.backoff();
      });
    },
    backoff: function backoff() {},
    // No-op
    success: function success() {},
    // No-op
    try: function _try() {},
    // No-op
    pause: function pause() {},
    // No-op
    unpause: function unpause() {
      return this.goto('TRY_ONE');
    }
  },

  TRY_ONE: {
    Enter: function Enter() {
      return this.try();
    },
    backoff: function backoff() {
      return this.goto('BACKOFF');
    },
    success: function success(connectionRdy) {
      this.backoffTimer.success();
      this.onMessageSuccess(connectionRdy);
      return this.goto('MAX');
    },
    try: function _try() {},
    // No-op
    pause: function pause() {
      return this.goto('PAUSE');
    },
    unpause: function unpause() {}
  }, // No-op

  MAX: {
    Enter: function Enter() {
      this.balance();
      return this.bump();
    },
    backoff: function backoff() {
      return this.goto('BACKOFF');
    },
    success: function success(connectionRdy) {
      this.backoffTimer.success();
      return this.onMessageSuccess(connectionRdy);
    },
    try: function _try() {},
    // No-op
    pause: function pause() {
      return this.goto('PAUSE');
    },
    unpause: function unpause() {}
  }, // No-op

  BACKOFF: {
    Enter: function Enter() {
      this.backoffTimer.failure();
      return this.backoff();
    },
    backoff: function backoff() {},
    // No-op
    success: function success() {},
    // No-op
    try: function _try() {
      return this.goto('TRY_ONE');
    },
    pause: function pause() {
      return this.goto('PAUSE');
    },
    unpause: function unpause() {}
  } // No-op
};

ReaderRdy.prototype.transitions = {
  '*': {
    '*': function _(data, callback) {
      this.log();
      return callback(data);
    }
  }
};

module.exports = { ReaderRdy: ReaderRdy, ConnectionRdy: ConnectionRdy };