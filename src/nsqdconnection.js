'use strict';

var _slicedToArray = function () { function sliceIterator(arr, i) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"]) _i["return"](); } finally { if (_d) throw _e; } } return _arr; } return function (arr, i) { if (Array.isArray(arr)) { return arr; } else if (Symbol.iterator in Object(arr)) { return sliceIterator(arr, i); } else { throw new TypeError("Invalid attempt to destructure non-iterable instance"); } }; }();

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _toConsumableArray(arr) { if (Array.isArray(arr)) { for (var i = 0, arr2 = Array(arr.length); i < arr.length; i++) { arr2[i] = arr[i]; } return arr2; } else { return Array.from(arr); } }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var _require = require('events'),
    EventEmitter = _require.EventEmitter;

var net = require('net');
var os = require('os');
var tls = require('tls');
var zlib = require('zlib');

var NodeState = require('node-state');
var _ = require('lodash');
var debug = require('debug');

var wire = require('./wire');
var FrameBuffer = require('./framebuffer');
var Message = require('./message');
var version = require('./version');

var _require2 = require('./config'),
    ConnectionConfig = _require2.ConnectionConfig;

/**
 * NSQDConnection is a reader connection to a nsqd instance. It manages all
 * aspects of the nsqd connection with the exception of the RDY count which
 * needs to be managed across all nsqd connections for a given topic / channel
 * pair.
 *
 * This shouldn't be used directly. Use a Reader instead.
 *
 * Usage:
 *   const c = new NSQDConnection('127.0.0.1', 4150, 'test', 'default', 60, 30)
 *
 *   c.on(NSQDConnection.MESSAGE, (msg) => {
 *     console.log(`[message]: ${msg.attempts}, ${msg.body.toString()}`)
 *     console.log(`Timeout of message is ${msg.timeUntilTimeout()}`)
 *     setTimeout(() => console.log(`${msg.timeUntilTimeout()}`), 5000)
 *     msg.finish()
 *   })
 *
 *   c.on(NSQDConnection.FINISHED, () =>  c.setRdy(1))
 *
 *   c.on(NSQDConnection.READY, () => {
 *     console.log('Callback [ready]: Set RDY to 100')
 *     c.setRdy(10)
 *   })
 *
 *   c.on(NSQDConnection.CLOSED, () => {
 *     console.log('Callback [closed]: Lost connection to nsqd')
 *   })
 *
 *   c.on(NSQDConnection.ERROR, (err) => {
 *     console.log(`Callback [error]: ${err}`)
 *   })
 *
 *   c.on(NSQDConnection.BACKOFF, () => {
 *     console.log('Callback [backoff]: RDY 0')
 *     c.setRdy(0)
 *     setTimeout(() => {
 *       c.setRdy 100;
 *       console.log('RDY 100')
 *     }, 10 * 1000)
 *   })
 *
 *   c.connect()
 */


var NSQDConnection = function (_EventEmitter) {
  _inherits(NSQDConnection, _EventEmitter);

  _createClass(NSQDConnection, null, [{
    key: 'BACKOFF',

    // Events emitted by NSQDConnection
    get: function get() {
      return 'backoff';
    }
  }, {
    key: 'CONNECTED',
    get: function get() {
      return 'connected';
    }
  }, {
    key: 'CLOSED',
    get: function get() {
      return 'closed';
    }
  }, {
    key: 'CONNECTION_ERROR',
    get: function get() {
      return 'connection_error';
    }
  }, {
    key: 'ERROR',
    get: function get() {
      return 'error';
    }
  }, {
    key: 'FINISHED',
    get: function get() {
      return 'finished';
    }
  }, {
    key: 'MESSAGE',
    get: function get() {
      return 'message';
    }
  }, {
    key: 'REQUEUED',
    get: function get() {
      return 'requeued';
    }
  }, {
    key: 'READY',
    get: function get() {
      return 'ready';
    }

    /**
     * Instantiates a new NSQDConnection.
     *
     * @constructor
     * @param  {String} nsqdHost
     * @param  {String|Number} nsqdPort
     * @param  {String} topic
     * @param  {String} channel
     * @param  {Object} [options={}]
     */

  }]);

  function NSQDConnection(nsqdHost, nsqdPort, topic, channel) {
    var options = arguments.length > 4 && arguments[4] !== undefined ? arguments[4] : {};

    _classCallCheck(this, NSQDConnection);

    var _this = _possibleConstructorReturn(this, (NSQDConnection.__proto__ || Object.getPrototypeOf(NSQDConnection)).call(this, nsqdHost, nsqdPort, topic, channel, options));

    _this.nsqdHost = nsqdHost;
    _this.nsqdPort = nsqdPort;
    _this.topic = topic;
    _this.channel = channel;
    var connId = _this.id().replace(':', '/');
    _this.debug = debug('nsqjs:reader:' + _this.topic + '/' + _this.channel + ':conn:' + connId);

    _this.config = new ConnectionConfig(options);
    _this.config.validate();

    _this.frameBuffer = new FrameBuffer();
    _this.statemachine = _this.connectionState();

    _this.maxRdyCount = 0; // Max RDY value for a conn to this NSQD
    _this.msgTimeout = 0; // Timeout time in milliseconds for a Message
    _this.maxMsgTimeout = 0; // Max time to process a Message in millisecs
    _this.nsqdVersion = null; // Version returned by nsqd
    _this.lastMessageTimestamp = null; // Timestamp of last message received
    _this.lastReceivedTimestamp = null; // Timestamp of last data received
    _this.conn = null; // Socket connection to NSQD
    _this.identifyTimeoutId = null; // Timeout ID for triggering identifyFail
    _this.messageCallbacks = []; // Callbacks on message sent responses
    _this.hadReconnectedCount = 0;
    return _this;
  }

  /**
   * The nsqd host:port pair.
   *
   * @return {[type]} [description]
   */


  _createClass(NSQDConnection, [{
    key: 'id',
    value: function id() {
      return this.nsqdHost + ':' + this.nsqdPort;
    }

    /**
     * Instantiates or returns a new ConnectionState.
     *
     * @return {ConnectionState}
     */

  }, {
    key: 'connectionState',
    value: function connectionState() {
      return this.statemachine || new ConnectionState(this);
    }

    /**
     * Creates a new nsqd connection.
     */

  }, {
    key: 'connect',
    value: function connect() {
      var _this2 = this;

      this.statemachine.raise('connecting');

      // Using nextTick so that clients of Reader can register event listeners
      // right after calling connect.
      process.nextTick(function () {
        _this2.conn = net.connect({ port: _this2.nsqdPort, host: _this2.nsqdHost }, function () {
          _this2.statemachine.raise('connected');
          _this2.emit(NSQDConnection.CONNECTED);
          _this2.hadReconnectedCount = 0;
          // Once there's a socket connection, give it 5 seconds to receive an
          // identify response.
          _this2.identifyTimeoutId = setTimeout(function () {
            _this2.identifyTimeout();
          }, 500);

          _this2.identifyTimeoutId;
        });
        _this2.conn.setNoDelay(true);

        _this2.registerStreamListeners(_this2.conn);
      });
    }
  }, {
    key: 'handleReconnect',
    value: function handleReconnect() {
      if (++this.hadReconnectedCount < this.config.maxReconnect) {
        this.debug('Reconnect...');
        this.connect();
      } else {
        throw new Error('Max reconnect retries(' + this.config.maxReconnect + ') reached');
      }
    }

    /**
     * Register event handlers for the nsqd connection.
     *
     * @param  {Object} conn
     */

  }, {
    key: 'registerStreamListeners',
    value: function registerStreamListeners(conn) {
      var _this3 = this;

      conn.on('data', function (data) {
        return _this3.receiveRawData(data);
      });
      conn.on('end', function () {
        try {
          _this3.handleReconnect();
        } catch (e) {
          _this3.statemachine.goto('CLOSED');
          throw e;
        }
      });
      conn.on('error', function (err) {
        try {
          _this3.handleReconnect();
        } catch (e) {
          _this3.statemachine.goto('ERROR', err);
          _this3.emit('connection_error', err);
          throw e;
        }
      });
      conn.on('close', function () {
        try {
          _this3.handleReconnect();
        } catch (e) {
          _this3.statemachine.raise('close');
          throw e;
        }
      });
      conn.setTimeout(this.config.idleTimeout * 1000, function () {
        return _this3.statemachine.raise('close');
      });
    }

    /**
     * Connect via tls.
     *
     * @param  {Function} callback
     */

  }, {
    key: 'startTLS',
    value: function startTLS(callback) {
      var _this4 = this;

      var _arr = ['data', 'error', 'close'];

      for (var _i = 0; _i < _arr.length; _i++) {
        var event = _arr[_i];
        this.conn.removeAllListeners(event);
      }

      var options = {
        socket: this.conn,
        rejectUnauthorized: this.config.tlsVerification,
        ca: this.config.ca,
        key: this.config.key,
        cert: this.config.cert
      };

      var tlsConn = tls.connect(options, function () {
        _this4.conn = tlsConn;
        typeof callback === 'function' ? callback() : undefined;
      });

      this.registerStreamListeners(tlsConn);
    }

    /**
     * Begin deflating the frame buffer. Actualy deflating is handled by
     * zlib.
     *
     * @param  {Number} level
     */

  }, {
    key: 'startDeflate',
    value: function startDeflate(level) {
      this.inflater = zlib.createInflateRaw({ flush: zlib.Z_SYNC_FLUSH });
      this.deflater = zlib.createDeflateRaw({ level: level, flush: zlib.Z_SYNC_FLUSH });
      this.reconsumeFrameBuffer();
    }

    /**
     * Create a snappy stream.
     */

  }, {
    key: 'startSnappy',
    value: function startSnappy() {
      var _require3 = require('snappystream'),
          SnappyStream = _require3.SnappyStream,
          UnsnappyStream = _require3.UnsnappyStream;

      this.inflater = new UnsnappyStream();
      this.deflater = new SnappyStream();
      this.reconsumeFrameBuffer();
    }

    /**
     * Consume the raw data from the frame buffer.
     */

  }, {
    key: 'reconsumeFrameBuffer',
    value: function reconsumeFrameBuffer() {
      if (this.frameBuffer.buffer && this.frameBuffer.buffer.length) {
        var data = this.frameBuffer.buffer;
        delete this.frameBuffer.buffer;
        this.receiveRawData(data);
      }
    }

    /**
     * Raise a `READY` event with the specified count.
     *
     * @param {Number} rdyCount
     */

  }, {
    key: 'setRdy',
    value: function setRdy(rdyCount) {
      this.statemachine.raise('ready', rdyCount);
    }

    /**
     * Handles reading the uncompressed payload from the inflater.
     *
     * @param  {Object} data
     * @return {undefined}
     */

  }, {
    key: 'receiveRawData',
    value: function receiveRawData(data) {
      var _this5 = this;

      if (!this.inflater) return this.receiveData(data);

      this.inflater.write(data, function () {
        var uncompressedData = _this5.inflater.read();
        if (uncompressedData) {
          _this5.receiveData(uncompressedData);
        }
      });
    }

    /**
     * Handle receiveing the message payload frame by frame.
     *
     * @param  {Object} data
     */

  }, {
    key: 'receiveData',
    value: function receiveData(data) {
      this.lastReceivedTimestamp = Date.now();
      this.frameBuffer.consume(data);

      var frame = this.frameBuffer.nextFrame();

      while (frame) {
        var _Array$from = Array.from(frame),
            _Array$from2 = _slicedToArray(_Array$from, 2),
            frameId = _Array$from2[0],
            payload = _Array$from2[1];

        switch (frameId) {
          case wire.FRAME_TYPE_RESPONSE:
            this.statemachine.raise('response', payload);
            break;
          case wire.FRAME_TYPE_ERROR:
            this.statemachine.goto('ERROR', new Error(payload.toString()));
            break;
          case wire.FRAME_TYPE_MESSAGE:
            this.lastMessageTimestamp = this.lastReceivedTimestamp;
            this.statemachine.raise('consumeMessage', this.createMessage(payload));
            break;
        }

        frame = this.frameBuffer.nextFrame();
      }
    }

    /**
     * Generates client metadata so that nsqd can identify connections.
     *
     * @return {Object} The connection metadata.
     */

  }, {
    key: 'identify',
    value: function identify() {
      var longName = os.hostname();
      var shortName = longName.split('.')[0];

      var identify = {
        client_id: this.config.clientId || shortName,
        deflate: this.config.deflate,
        deflate_level: this.config.deflateLevel,
        feature_negotiation: true,
        heartbeat_interval: this.config.heartbeatInterval * 1000,
        hostname: longName,
        long_id: longName, // Remove when deprecating pre 1.0
        msg_timeout: this.config.messageTimeout,
        output_buffer_size: this.config.outputBufferSize,
        output_buffer_timeout: this.config.outputBufferTimeout,
        sample_rate: this.config.sampleRate,
        short_id: shortName, // Remove when deprecating pre 1.0
        snappy: this.config.snappy,
        tls_v1: this.config.tls,
        user_agent: 'nsqjs/' + version

        // Remove some keys when they're effectively not provided.
      };var removableKeys = ['msg_timeout', 'output_buffer_size', 'output_buffer_timeout', 'sample_rate'];

      removableKeys.forEach(function (key) {
        if (identify[key] === null) {
          delete identify[key];
        }
      });

      return identify;
    }

    /**
     * Throws an error if the connection timed out while identifying the nsqd.
     */

  }, {
    key: 'identifyTimeout',
    value: function identifyTimeout() {
      this.statemachine.goto('ERROR', new Error('Timed out identifying with nsqd'));
    }

    /**
     * Clears an identify timeout. Useful for retries.
     */

  }, {
    key: 'clearIdentifyTimeout',
    value: function clearIdentifyTimeout() {
      clearTimeout(this.identifyTimeoutId);
      this.identifyTimeoutId = null;
    }

    /**
     * Create a new message from the payload.
     *
     * @param  {Buffer} msgPayload
     * @return {Message}
     */

  }, {
    key: 'createMessage',
    value: function createMessage(msgPayload) {
      var _this6 = this;

      var msgComponents = wire.unpackMessage(msgPayload);
      var msg = new (Function.prototype.bind.apply(Message, [null].concat(_toConsumableArray(msgComponents), [this.config.requeueDelay, this.msgTimeout, this.maxMsgTimeout])))();

      this.debug('Received message [' + msg.id + '] [attempts: ' + msg.attempts + ']');

      msg.on(Message.RESPOND, function (responseType, wireData) {
        _this6.write(wireData);

        if (responseType === Message.FINISH) {
          _this6.debug('Finished message [' + msg.id + '] [timedout=' + (msg.timedout === true) + ', elapsed=' + (Date.now() - msg.receivedOn) + 'ms, touch_count=' + msg.touchCount + ']');
          _this6.emit(NSQDConnection.FINISHED);
        } else if (responseType === Message.REQUEUE) {
          _this6.debug('Requeued message [' + msg.id + ']');
          _this6.emit(NSQDConnection.REQUEUED);
        }
      });

      msg.on(Message.BACKOFF, function () {
        return _this6.emit(NSQDConnection.BACKOFF);
      });

      return msg;
    }

    /**
     * Write a message to the connection. Deflate it if necessary.
     * @param  {Object} data
     */

  }, {
    key: 'write',
    value: function write(data) {
      var _this7 = this;

      if (this.deflater) {
        this.deflater.write(data, function () {
          return _this7.conn.write(_this7.deflater.read());
        });
      } else {
        this.conn.write(data);
      }
    }

    /**
     * Close the nsqd connection.
     */

  }, {
    key: 'close',
    value: function close() {
      if (!this.conn.destroyed && this.statemachine.current_state != 'CLOSED' && this.statemachine.current_state != 'ERROR') {
        try {
          this.conn.end(wire.close());
        } catch (e) {}
      }
      this.statemachine.goto('CLOSED');
    }

    /**
     * Destroy the nsqd connection.
     */

  }, {
    key: 'destroy',
    value: function destroy() {
      if (!this.conn.destroyed) {
        this.conn.destroy();
      }
    }
  }]);

  return NSQDConnection;
}(EventEmitter);

/**
 * A statemachine modeling the connection state of an nsqd connection.
 * @type {ConnectionState}
 */


var ConnectionState = function (_NodeState) {
  _inherits(ConnectionState, _NodeState);

  /**
   * Instantiates a new instance of ConnectionState.
   *
   * @constructor
   * @param  {Object} conn
   */
  function ConnectionState(conn) {
    _classCallCheck(this, ConnectionState);

    var _this8 = _possibleConstructorReturn(this, (ConnectionState.__proto__ || Object.getPrototypeOf(ConnectionState)).call(this, {
      autostart: true,
      initial_state: 'INIT',
      sync_goto: true
    }));

    _this8.conn = conn;
    _this8.identifyResponse = null;
    return _this8;
  }

  /**
   * @param  {*} message
   */


  _createClass(ConnectionState, [{
    key: 'log',
    value: function log(message) {
      if (this.current_state_name !== 'INIT') {
        this.conn.debug('' + this.current_state_name);
      }
      if (message) {
        this.conn.debug(message);
      }
    }

    /**
     * @return {String}
     */

  }, {
    key: 'afterIdentify',
    value: function afterIdentify() {
      return 'SUBSCRIBE';
    }
  }]);

  return ConnectionState;
}(NodeState);

ConnectionState.prototype.states = {
  INIT: {
    connecting: function connecting() {
      return this.goto('CONNECTING');
    }
  },

  CONNECTING: {
    connected: function connected() {
      return this.goto('CONNECTED');
    }
  },

  CONNECTED: {
    Enter: function Enter() {
      return this.goto('SEND_MAGIC_IDENTIFIER');
    }
  },

  SEND_MAGIC_IDENTIFIER: {
    Enter: function Enter() {
      // Send the magic protocol identifier to the connection
      this.conn.write(wire.MAGIC_V2);
      return this.goto('IDENTIFY');
    }
  },

  IDENTIFY: {
    Enter: function Enter() {
      // Send configuration details
      var identify = this.conn.identify();
      this.conn.debug(identify);
      this.conn.write(wire.identify(identify));
      return this.goto('IDENTIFY_RESPONSE');
    }
  },

  IDENTIFY_RESPONSE: {
    response: function response(data) {
      if (data.toString() === 'OK') {
        data = JSON.stringify({
          max_rdy_count: 2500,
          max_msg_timeout: 15 * 60 * 1000, // 15 minutes
          msg_timeout: 60 * 1000
        }); //  1 minute
      }

      this.identifyResponse = JSON.parse(data);
      this.conn.debug(this.identifyResponse);
      this.conn.maxRdyCount = this.identifyResponse.max_rdy_count;
      this.conn.maxMsgTimeout = this.identifyResponse.max_msg_timeout;
      this.conn.msgTimeout = this.identifyResponse.msg_timeout;
      this.conn.nsqdVersion = this.identifyResponse.version;
      this.conn.clearIdentifyTimeout();

      if (this.identifyResponse.tls_v1) {
        return this.goto('TLS_START');
      }
      return this.goto('IDENTIFY_COMPRESSION_CHECK');
    }
  },

  IDENTIFY_COMPRESSION_CHECK: {
    Enter: function Enter() {
      var _identifyResponse = this.identifyResponse,
          deflate = _identifyResponse.deflate,
          snappy = _identifyResponse.snappy;


      if (deflate) {
        return this.goto('DEFLATE_START', this.identifyResponse.deflate_level);
      }
      if (snappy) {
        return this.goto('SNAPPY_START');
      }
      return this.goto('AUTH');
    }
  },

  TLS_START: {
    Enter: function Enter() {
      this.conn.startTLS();
      return this.goto('TLS_RESPONSE');
    }
  },

  TLS_RESPONSE: {
    response: function response(data) {
      if (data.toString() === 'OK') {
        return this.goto('IDENTIFY_COMPRESSION_CHECK');
      }
      return this.goto('ERROR', new Error('TLS negotiate error with nsqd'));
    }
  },

  DEFLATE_START: {
    Enter: function Enter(level) {
      this.conn.startDeflate(level);
      return this.goto('COMPRESSION_RESPONSE');
    }
  },

  SNAPPY_START: {
    Enter: function Enter() {
      this.conn.startSnappy();
      return this.goto('COMPRESSION_RESPONSE');
    }
  },

  COMPRESSION_RESPONSE: {
    response: function response(data) {
      if (data.toString() === 'OK') {
        return this.goto('AUTH');
      }
      return this.goto('ERROR', new Error('Bad response when enabling compression'));
    }
  },

  AUTH: {
    Enter: function Enter() {
      if (!this.conn.config.authSecret) {
        return this.goto(this.afterIdentify());
      }
      this.conn.write(wire.auth(this.conn.config.authSecret));
      return this.goto('AUTH_RESPONSE');
    }
  },

  AUTH_RESPONSE: {
    response: function response(data) {
      this.conn.auth = JSON.parse(data);
      return this.goto(this.afterIdentify());
    }
  },

  SUBSCRIBE: {
    Enter: function Enter() {
      this.conn.write(wire.subscribe(this.conn.topic, this.conn.channel));
      return this.goto('SUBSCRIBE_RESPONSE');
    }
  },

  SUBSCRIBE_RESPONSE: {
    response: function response(data) {
      if (data.toString() === 'OK') {
        this.goto('READY_RECV');
        // Notify listener that this nsqd connection has passed the subscribe
        // phase. Do this only once for a connection.
        return this.conn.emit(NSQDConnection.READY);
      }
    }
  },

  READY_RECV: {
    consumeMessage: function consumeMessage(msg) {
      return this.conn.emit(NSQDConnection.MESSAGE, msg);
    },
    response: function response(data) {
      if (data.toString() === '_heartbeat_') {
        return this.conn.write(wire.nop());
      }
    },
    ready: function ready(rdyCount) {
      // RDY count for this nsqd cannot exceed the nsqd configured
      // max rdy count.
      if (rdyCount > this.conn.maxRdyCount) {
        rdyCount = this.conn.maxRdyCount;
      }
      return this.conn.write(wire.ready(rdyCount));
    },
    close: function close() {
      return this.goto('CLOSED');
    }
  },

  READY_SEND: {
    Enter: function Enter() {
      // Notify listener that this nsqd connection is ready to send.
      return this.conn.emit(NSQDConnection.READY);
    },
    produceMessages: function produceMessages(data) {
      var _Array$from3 = Array.from(data),
          _Array$from4 = _slicedToArray(_Array$from3, 4),
          topic = _Array$from4[0],
          msgs = _Array$from4[1],
          timeMs = _Array$from4[2],
          callback = _Array$from4[3];

      this.conn.messageCallbacks.push(callback);

      if (!_.isArray(msgs)) {
        throw new Error('Expect an array of messages to produceMessages');
      }

      if (msgs.length === 1) {
        if (!timeMs) {
          return this.conn.write(wire.pub(topic, msgs[0]));
        } else {
          return this.conn.write(wire.dpub(topic, msgs[0], timeMs));
        }
      }
      if (!timeMs) {
        return this.conn.write(wire.mpub(topic, msgs));
      } else {
        throw new Error('DPUB can only defer one message at a time');
      }
    },
    response: function response(data) {
      switch (data.toString()) {
        case 'OK':
          var cb = this.conn.messageCallbacks.shift();
          return typeof cb === 'function' ? cb(null) : undefined;
        case '_heartbeat_':
          return this.conn.write(wire.nop());
      }
    },
    close: function close() {
      return this.goto('CLOSED');
    }
  },

  ERROR: {
    Enter: function Enter(err) {
      // If there's a callback, pass it the error.
      var cb = this.conn.messageCallbacks.shift();
      if (typeof cb === 'function') {
        cb(err);
      }

      this.conn.emit(NSQDConnection.ERROR, err);

      // According to NSQ docs, the following errors are non-fatal and should
      // not close the connection. See here for more info:
      // http://nsq.io/clients/building_client_libraries.html
      if (!_.isString(err)) {
        err = err.toString();
      }
      var errorCode = err.split(/\s+/)[1];

      if (['E_REQ_FAILED', 'E_FIN_FAILED', 'E_TOUCH_FAILED'].includes(errorCode)) {
        return this.goto('READY_RECV');
      }
      return this.goto('CLOSED');
    },
    close: function close() {
      return this.goto('CLOSED');
    }
  },

  CLOSED: {
    Enter: function Enter() {
      if (!this.conn) {
        return;
      }

      // If there are callbacks, then let them error on the closed connection.
      var err = new Error('nsqd connection closed');
      var _iteratorNormalCompletion = true;
      var _didIteratorError = false;
      var _iteratorError = undefined;

      try {
        for (var _iterator = this.conn.messageCallbacks[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
          var cb = _step.value;

          if (typeof cb === 'function') {
            cb(err);
          }
        }
      } catch (err) {
        _didIteratorError = true;
        _iteratorError = err;
      } finally {
        try {
          if (!_iteratorNormalCompletion && _iterator.return) {
            _iterator.return();
          }
        } finally {
          if (_didIteratorError) {
            throw _iteratorError;
          }
        }
      }

      this.conn.messageCallbacks = [];
      this.disable();
      this.conn.destroy();
      this.conn.emit(NSQDConnection.CLOSED);
      return delete this.conn;
    },


    // No-op. Once closed, subsequent calls should do nothing.
    close: function close() {}
  }
};

ConnectionState.prototype.transitions = {
  '*': {
    '*': function _(data, callback) {
      this.log();
      return callback(data);
    },

    CONNECTED: function CONNECTED(data, callback) {
      this.log();
      return callback(data);
    },
    ERROR: function ERROR(err, callback) {
      this.log('' + err);
      return callback(err);
    }
  }

  /**
   * WriterConnectionState
   *
   * Usage:
   *   c = new NSQDConnectionWriter '127.0.0.1', 4150, 30
   *   c.connect()
   *
   *   c.on NSQDConnectionWriter.CLOSED, ->
   *     console.log "Callback [closed]: Lost connection to nsqd"
   *
   *   c.on NSQDConnectionWriter.ERROR, (err) ->
   *     console.log "Callback [error]: #{err}"
   *
   *   c.on NSQDConnectionWriter.READY, ->
   *     c.produceMessages 'sample_topic', ['first message']
   *     c.produceMessages 'sample_topic', ['second message', 'third message']
   *     c.destroy()
   */
};
var WriterNSQDConnection = function (_NSQDConnection) {
  _inherits(WriterNSQDConnection, _NSQDConnection);

  /**
   * @constructor
   * @param  {String} nsqdHost
   * @param  {String|Number} nsqdPort
   * @param  {Object} [options={}]
   */
  function WriterNSQDConnection(nsqdHost, nsqdPort) {
    var options = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {};

    _classCallCheck(this, WriterNSQDConnection);

    var _this9 = _possibleConstructorReturn(this, (WriterNSQDConnection.__proto__ || Object.getPrototypeOf(WriterNSQDConnection)).call(this, nsqdHost, nsqdPort, null, null, options));

    _this9.debug = debug('nsqjs:writer:conn:' + nsqdHost + '/' + nsqdPort);
    return _this9;
  }

  /**
   * Instantiates a new instance of WriterConnectionState or returns an
   * existing one.
   *
   * @return {WriterConnectionState}
   */


  _createClass(WriterNSQDConnection, [{
    key: 'connectionState',
    value: function connectionState() {
      return this.statemachine || new WriterConnectionState(this);
    }

    /**
     * Emits a `produceMessages` event with the specified topic, msgs, timeMs and a
     * callback.
     *
     * @param  {String}   topic
     * @param  {Array}    msgs
     * @param  {Number}   timeMs
     * @param  {Function} callback
     */

  }, {
    key: 'produceMessages',
    value: function produceMessages(topic, msgs, timeMs, callback) {
      this.statemachine.raise('produceMessages', [topic, msgs, timeMs, callback]);
    }
  }]);

  return WriterNSQDConnection;
}(NSQDConnection);

/**
 * A statemachine modeling the various states a writer connection can be in.
 */


var WriterConnectionState = function (_ConnectionState) {
  _inherits(WriterConnectionState, _ConnectionState);

  function WriterConnectionState() {
    _classCallCheck(this, WriterConnectionState);

    return _possibleConstructorReturn(this, (WriterConnectionState.__proto__ || Object.getPrototypeOf(WriterConnectionState)).apply(this, arguments));
  }

  _createClass(WriterConnectionState, [{
    key: 'afterIdentify',

    /**
     * Returned when the connection is ready to send messages.
     *
     * @return {String}
     */
    value: function afterIdentify() {
      return 'READY_SEND';
    }
  }]);

  return WriterConnectionState;
}(ConnectionState);

module.exports = {
  NSQDConnection: NSQDConnection,
  ConnectionState: ConnectionState,
  WriterNSQDConnection: WriterNSQDConnection,
  WriterConnectionState: WriterConnectionState
};