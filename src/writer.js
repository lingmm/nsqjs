'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var _ = require('lodash');
var debug = require('debug');

var _require = require('events'),
    EventEmitter = _require.EventEmitter;

var _require2 = require('./config'),
    ConnectionConfig = _require2.ConnectionConfig;

var _require3 = require('./nsqdconnection'),
    WriterNSQDConnection = _require3.WriterNSQDConnection;

/**
 *  Publish messages to nsqds.
 *
 *  Usage:
 *    const writer = new Writer('127.0.0.1', 4150);
 *    writer.connect();
 *
 *    writer.on(Writer.READY, () => {
 *      // Send a single message
 *      writer.publish('sample_topic', 'one');
 *      // Send multiple messages
 *      writer.publish('sample_topic', ['two', 'three']);
 *    });
 *
 *    writer.on(Writer.CLOSED, () => {
 *      console.log('Writer closed');
 *    });
 */


var Writer = function (_EventEmitter) {
  _inherits(Writer, _EventEmitter);

  _createClass(Writer, null, [{
    key: 'READY',

    // Writer events
    get: function get() {
      return 'ready';
    }
  }, {
    key: 'CLOSED',
    get: function get() {
      return 'closed';
    }
  }, {
    key: 'ERROR',
    get: function get() {
      return 'error';
    }

    /**
     * Instantiates a new Writer.
     *
     * @constructor
     * @param {String} nsqdHost
     * @param {String} nsqdPort
     * @param {Object} options
     */

  }]);

  function Writer(nsqdHost, nsqdPort, options) {
    _classCallCheck(this, Writer);

    var _this = _possibleConstructorReturn(this, (Writer.__proto__ || Object.getPrototypeOf(Writer)).call(this));

    _this.nsqdHost = nsqdHost;
    _this.nsqdPort = nsqdPort;

    // Handy in the event that there are tons of publish calls
    // while the Writer is connecting.
    _this.setMaxListeners(10000);

    _this.debug = debug('nsqjs:writer:' + _this.nsqdHost + '/' + _this.nsqdPort);
    _this.config = new ConnectionConfig(options);
    _this.config.validate();
    _this.ready = false;

    _this.debug('Configuration');
    _this.debug(_this.config);
    return _this;
  }

  /**
   * Connect establishes a new nsqd writer connection.
   */


  _createClass(Writer, [{
    key: 'connect',
    value: function connect() {
      var _this2 = this;

      this.conn = new WriterNSQDConnection(this.nsqdHost, this.nsqdPort, this.config);

      this.debug('connect');
      this.conn.connect();

      this.conn.on(WriterNSQDConnection.READY, function () {
        _this2.debug('ready');
        _this2.ready = true;
        _this2.emit(Writer.READY);
      });

      this.conn.on(WriterNSQDConnection.CLOSED, function () {
        _this2.debug('closed');
        _this2.ready = false;
        _this2.emit(Writer.CLOSED);
      });

      this.conn.on(WriterNSQDConnection.ERROR, function (err) {
        _this2.debug('error', err);
        _this2.ready = false;
        _this2.emit(Writer.ERROR, err);
      });

      this.conn.on(WriterNSQDConnection.CONNECTION_ERROR, function (err) {
        _this2.debug('error', err);
        _this2.ready = false;
        _this2.emit(Writer.ERROR, err);
      });
    }

    /**
     * Publish a message or a list of messages to the connected nsqd. The contents
     * of the messages should either be strings or buffers with the payload encoded.
      * @param {String} topic
     * @param {String|Buffer|Object|Array} msgs - A string, a buffer, a
     *   JSON serializable object, or a list of string / buffers /
     *   JSON serializable objects.
     * @param {Function} callback
     * @return {undefined}
     */

  }, {
    key: 'publish',
    value: function publish(topic, msgs, callback) {
      var _this3 = this;

      var err = this._checkStateValidity();
      err = err || this._checkMsgsValidity(msgs);

      if (err) {
        return this._throwOrCallback(err, callback);
      }

      // Call publish again once the Writer is ready.
      if (!this.ready) {
        var onReady = function onReady(err) {
          if (err) return callback(err);
          _this3.publish(topic, msgs, callback);
        };
        this._callwhenReady(onReady);
      }

      if (!_.isArray(msgs)) {
        msgs = [msgs];
      }

      // Automatically serialize as JSON if the message isn't a String or a Buffer
      msgs = msgs.map(this._serializeMsg);

      return this.conn.produceMessages(topic, msgs, undefined, callback);
    }

    /**
     * Publish a message to the connected nsqd with delay. The contents
     * of the messages should either be strings or buffers with the payload encoded.
      * @param {String} topic
     * @param {String|Buffer|Object} msg - A string, a buffer, a
     *   JSON serializable object, or a list of string / buffers /
     *   JSON serializable objects.
     * @param {Number} timeMs - defer time
     * @param {Function} callback
     * @return {undefined}
     */

  }, {
    key: 'deferPublish',
    value: function deferPublish(topic, msg, timeMs, callback) {
      var _this4 = this;

      var err = this._checkStateValidity();
      err = err || this._checkMsgsValidity(msg);
      err = err || this._checkTimeMsValidity(timeMs);

      if (err) {
        return this._throwOrCallback(err, callback);
      }

      // Call publish again once the Writer is ready.
      if (!this.ready) {
        var onReady = function onReady(err) {
          if (err) return callback(err);
          _this4.deferPublish(topic, msg, timeMs, callback);
        };
        this._callwhenReady(onReady);
      }

      return this.conn.produceMessages(topic, msg, timeMs, callback);
    }

    /**
     * Close the writer connection.
     * @return {undefined}
     */

  }, {
    key: 'close',
    value: function close() {
      return this.conn.close();
    }
  }, {
    key: '_serializeMsg',
    value: function _serializeMsg(msg) {
      if (_.isString(msg) || Buffer.isBuffer(msg)) {
        return msg;
      }
      return JSON.stringify(msg);
    }
  }, {
    key: '_checkStateValidity',
    value: function _checkStateValidity() {
      var connState = '';

      if (this.conn && this.conn.statemachine) {
        connState = this.conn.statemachine.current_state_name;
      }

      if (!this.conn || ['CLOSED', 'ERROR'].includes(connState)) {
        return new Error('No active Writer connection to send messages');
      }
    }
  }, {
    key: '_checkMsgsValidity',
    value: function _checkMsgsValidity(msgs) {
      // maybe when an array check every message to not be empty
      if (!msgs || _.isEmpty(msgs)) {
        return new Error('Attempting to publish an empty message');
      }
    }
  }, {
    key: '_checkTimeMsValidity',
    value: function _checkTimeMsValidity(timeMs) {
      return _.isNumber(timeMs) && timeMs > 0 ? undefined : new Error('The Delay must be a (positiv) number');
    }
  }, {
    key: '_throwOrCallback',
    value: function _throwOrCallback(err, callback) {
      if (callback) {
        return callback(err);
      }
      throw err;
    }
  }, {
    key: '_callwhenReady',
    value: function _callwhenReady(fn) {
      var _this5 = this;

      var ready = function ready() {
        remove();
        fn();
      };

      var failed = function failed(err) {
        if (!err) {
          err = new Error('Connection closed!');
        }
        remove();
        fn(err);
      };

      var remove = function remove() {
        _this5.removeListener(Writer.READY, ready);
        _this5.removeListener(Writer.ERROR, failed);
        _this5.removeListener(Writer.CLOSED, failed);
      };

      this.on(Writer.READY, ready);
      this.on(Writer.ERROR, failed);
      this.on(Writer.CLOSED, failed);
    }
  }]);

  return Writer;
}(EventEmitter);

module.exports = Writer;