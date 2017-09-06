'use strict';

var _slicedToArray = function () { function sliceIterator(arr, i) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"]) _i["return"](); } finally { if (_d) throw _e; } } return _arr; } return function (arr, i) { if (Array.isArray(arr)) { return arr; } else if (Symbol.iterator in Object(arr)) { return sliceIterator(arr, i); } else { throw new TypeError("Invalid attempt to destructure non-iterable instance"); } }; }();

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var _require = require('events'),
    EventEmitter = _require.EventEmitter;

var debug = require('debug');

var RoundRobinList = require('./roundrobinlist');
var lookup = require('./lookupd');

var _require2 = require('./nsqdconnection'),
    NSQDConnection = _require2.NSQDConnection;

var _require3 = require('./config'),
    ReaderConfig = _require3.ReaderConfig;

var _require4 = require('./readerrdy'),
    ReaderRdy = _require4.ReaderRdy;

/**
 * Reader provides high-level functionality for building robust NSQ
 * consumers. Reader is built upon the EventEmitter and thus supports various
 * hooks when different events occur.
 * @type {Reader}
 */


var Reader = function (_EventEmitter) {
  _inherits(Reader, _EventEmitter);

  _createClass(Reader, null, [{
    key: 'ERROR',
    get: function get() {
      return 'error';
    }
  }, {
    key: 'MESSAGE',
    get: function get() {
      return 'message';
    }
  }, {
    key: 'DISCARD',
    get: function get() {
      return 'discard';
    }
  }, {
    key: 'NSQD_CONNECTED',
    get: function get() {
      return 'nsqd_connected';
    }
  }, {
    key: 'NSQD_CLOSED',
    get: function get() {
      return 'nsqd_closed';
    }

    /**
     * @constructor
     * @param  {String} topic
     * @param  {String} channel
     * @param  {Object} options
     */

  }]);

  function Reader(topic, channel, options) {
    var _ref;

    _classCallCheck(this, Reader);

    for (var _len = arguments.length, args = Array(_len > 3 ? _len - 3 : 0), _key = 3; _key < _len; _key++) {
      args[_key - 3] = arguments[_key];
    }

    var _this = _possibleConstructorReturn(this, (_ref = Reader.__proto__ || Object.getPrototypeOf(Reader)).call.apply(_ref, [this, topic, channel, options].concat(args)));

    _this.topic = topic;
    _this.channel = channel;
    _this.debug = debug('nsqjs:reader:' + _this.topic + '/' + _this.channel);
    _this.config = new ReaderConfig(options);
    _this.config.validate();

    _this.debug('Configuration');
    _this.debug(_this.config);

    _this.roundrobinLookupd = new RoundRobinList(_this.config.lookupdHTTPAddresses);

    _this.readerRdy = new ReaderRdy(_this.config.maxInFlight, _this.config.maxBackoffDuration, _this.topic + '/' + _this.channel);

    _this.connectIntervalId = null;
    _this.connectionIds = [];
    return _this;
  }

  /**
   * Adds a connection to nsqd at the configured address.
   *
   * @return {undefined}
   */


  _createClass(Reader, [{
    key: 'connect',
    value: function connect() {
      var _this2 = this;

      var delayedStart = void 0;
      var interval = this.config.lookupdPollInterval * 1000;
      var delay = Math.random() * this.config.lookupdPollJitter * interval;

      // Connect to provided nsqds.
      if (this.config.nsqdTCPAddresses.length) {
        var directConnect = function directConnect() {
          // Don't establish new connections while the Reader is paused.
          if (_this2.isPaused()) return;

          if (_this2.connectionIds.length < _this2.config.nsqdTCPAddresses.length) {
            return _this2.config.nsqdTCPAddresses.forEach(function (addr) {
              var _addr$split = addr.split(':'),
                  _addr$split2 = _slicedToArray(_addr$split, 2),
                  address = _addr$split2[0],
                  port = _addr$split2[1];

              _this2.connectToNSQD(address, Number(port));
            });
          }
        };

        delayedStart = function delayedStart() {
          _this2.connectIntervalId = setInterval(directConnect.bind(_this2), interval);
        };

        // Connect immediately.
        directConnect();

        // Start interval for connecting after delay.
        setTimeout(delayedStart, delay).unref();
      }

      delayedStart = function delayedStart() {
        _this2.connectIntervalId = setInterval(_this2.queryLookupd.bind(_this2), interval);
      };

      // Connect immediately.
      this.queryLookupd();

      // Start interval for querying lookupd after delay.
      setTimeout(delayedStart, delay);
    }

    /**
     * Close all connections and prevent any periodic callbacks.
     * @return {Array} The closed connections.
     */

  }, {
    key: 'close',
    value: function close() {
      clearInterval(this.connectIntervalId);
      return this.readerRdy.close();
    }

    /**
     * Pause all connections
     * @return {Array} The paused connections.
     */

  }, {
    key: 'pause',
    value: function pause() {
      this.debug('pause');
      return this.readerRdy.pause();
    }

    /**
     * Unpause all connections
     * @return {Array} The unpaused connections.
     */

  }, {
    key: 'unpause',
    value: function unpause() {
      this.debug('unpause');
      return this.readerRdy.unpause();
    }

    /**
     * @return {Boolean}
     */

  }, {
    key: 'isPaused',
    value: function isPaused() {
      return this.readerRdy.isPaused();
    }

    /**
     * Trigger a query of the configured nsq_lookupd_http_addresses.
     * @return {undefined}
     */

  }, {
    key: 'queryLookupd',
    value: function queryLookupd() {
      var _this3 = this;

      // Don't establish new connections while the Reader is paused.
      if (this.isPaused()) return;

      // Trigger a query of the configured `lookupdHTTPAddresses`.
      var endpoint = this.roundrobinLookupd.next();
      lookup(endpoint, this.topic, function (err) {
        var nodes = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : [];
        return nodes.map(function (n) {
          _this3.connectToNSQD(n.broadcast_address || n.hostname, n.tcp_port);
        });
      });
    }

    /**
     * Adds a connection to nsqd at the specified address.
     *
     * @param  {String} host
     * @param  {Number|String} port
     * @return {Object|undefined} The newly created nsqd connection.
     */

  }, {
    key: 'connectToNSQD',
    value: function connectToNSQD(host, port) {
      this.debug('discovered ' + host + ':' + port + ' for ' + this.topic + ' topic');
      var conn = new NSQDConnection(host, port, this.topic, this.channel, this.config);

      // Ensure a connection doesn't already exist to this nsqd instance.
      if (this.connectionIds.indexOf(conn.id()) !== -1) {
        return;
      }

      this.debug('connecting to ' + host + ':' + port);
      this.connectionIds.push(conn.id());

      this.registerConnectionListeners(conn);
      this.readerRdy.addConnection(conn);

      return conn.connect();
    }

    /**
     * Registers event handlers for the nsqd connection.
     * @param  {Object} conn
     */

  }, {
    key: 'registerConnectionListeners',
    value: function registerConnectionListeners(conn) {
      var _this4 = this;

      conn.on(NSQDConnection.CONNECTED, function () {
        _this4.debug(Reader.NSQD_CONNECTED);
        _this4.emit(Reader.NSQD_CONNECTED, conn.nsqdHost, conn.nsqdPort);
      });

      conn.on(NSQDConnection.ERROR, function (err) {
        _this4.debug(Reader.ERROR);
        _this4.debug(err);
        _this4.emit(Reader.ERROR, err);
      });

      conn.on(NSQDConnection.CONNECTION_ERROR, function (err) {
        _this4.debug(Reader.ERROR);
        _this4.debug(err);
        _this4.emit(Reader.ERROR, err);
      });

      // On close, remove the connection id from this reader.
      conn.on(NSQDConnection.CLOSED, function () {
        _this4.debug(Reader.NSQD_CLOSED);

        var index = _this4.connectionIds.indexOf(conn.id());
        if (index === -1) {
          return;
        }
        _this4.connectionIds.splice(index, 1);

        _this4.emit(Reader.NSQD_CLOSED, conn.nsqdHost, conn.nsqdPort);
      });

      /**
       * On message, send either a message or discard event depending on the
       * number of attempts.
       */
      conn.on(NSQDConnection.MESSAGE, function (message) {
        _this4.handleMessage(message);
      });
    }

    /**
     * Asynchronously handles an nsqd message.
     *
     * @param  {Object} message
     */

  }, {
    key: 'handleMessage',
    value: function handleMessage(message) {
      var _this5 = this;

      /**
       * Give the internal event listeners a chance at the events
       * before clients of the Reader.
       */
      process.nextTick(function () {
        var autoFinishMessage = _this5.config.maxAttempts > 0 && _this5.config.maxAttempts <= message.attempts;
        var numDiscardListeners = _this5.listeners(Reader.DISCARD).length;

        if (autoFinishMessage && numDiscardListeners > 0) {
          _this5.emit(Reader.DISCARD, message);
        } else {
          _this5.emit(Reader.MESSAGE, message);
        }

        if (autoFinishMessage) {
          message.finish();
        }
      });
    }
  }]);

  return Reader;
}(EventEmitter);

module.exports = Reader;