'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var _require = require('events'),
    EventEmitter = _require.EventEmitter;

var wire = require('./wire');

/**
 * Message - a high-level message object, which exposes stateful methods
 * for responding to nsqd (FIN, REQ, TOUCH, etc.) as well as metadata
 * such as attempts and timestamp.
 * @type {Message}
 */

var Message = function (_EventEmitter) {
  _inherits(Message, _EventEmitter);

  _createClass(Message, null, [{
    key: 'BACKOFF',

    // Event types
    get: function get() {
      return 'backoff';
    }
  }, {
    key: 'RESPOND',
    get: function get() {
      return 'respond';
    }

    // Response types

  }, {
    key: 'FINISH',
    get: function get() {
      return 0;
    }
  }, {
    key: 'REQUEUE',
    get: function get() {
      return 1;
    }
  }, {
    key: 'TOUCH',
    get: function get() {
      return 2;
    }

    /**
     * Instantiates a new instance of a Message.
     * @constructor
     * @param  {String} id
     * @param  {String|Number} timestamp
     * @param  {Number} attempts
     * @param  {String} body
     * @param  {Number} requeueDelay
     * @param  {Number} msgTimeout
     * @param  {Number} maxMsgTimeout
     */

  }]);

  function Message(id, timestamp, attempts, body, requeueDelay, msgTimeout, maxMsgTimeout) {
    _classCallCheck(this, Message);

    // eslint-disable-line prefer-rest-params
    var _this = _possibleConstructorReturn(this, (Message.__proto__ || Object.getPrototypeOf(Message)).apply(this, arguments));

    _this.id = id;
    _this.timestamp = timestamp;
    _this.attempts = attempts;
    _this.body = body;
    _this.requeueDelay = requeueDelay;
    _this.msgTimeout = msgTimeout;
    _this.maxMsgTimeout = maxMsgTimeout;
    _this.hasResponded = false;
    _this.receivedOn = Date.now();
    _this.lastTouched = _this.receivedOn;
    _this.touchCount = 0;
    _this.trackTimeoutId = null;

    // Keep track of when this message actually times out.
    _this.timedOut = false;
    _this.trackTimeout();
    return _this;
  }

  /**
   * track whether or not a message has timed out.
   */


  _createClass(Message, [{
    key: 'trackTimeout',
    value: function trackTimeout() {
      var _this2 = this;

      if (this.hasResponded) return;

      var soft = this.timeUntilTimeout();
      var hard = this.timeUntilTimeout(true);

      // Both values have to be not null otherwise we've timedout.
      this.timedOut = !soft || !hard;
      if (!this.timedOut) {
        clearTimeout(this.trackTimeoutId);
        this.trackTimeoutId = setTimeout(function () {
          return _this2.trackTimeout();
        }, Math.min(soft, hard)).unref();
      }
    }

    /**
     * Safely parse the body into JSON.
     *
     * @return {Object}
     */

  }, {
    key: 'json',
    value: function json() {
      if (this.parsed == null) {
        try {
          this.parsed = JSON.parse(this.body);
        } catch (err) {
          throw new Error('Invalid JSON in Message');
        }
      }

      return this.parsed;
    }

    /**
     * Returns in milliseconds the time until this message expires. Returns
     * null if that time has already ellapsed. There are two different timeouts
     * for a message. There are the soft timeouts that can be extended by touching
     * the message. There is the hard timeout that cannot be exceeded without
     * reconfiguring the nsqd.
     *
     * @param  {Boolean} [hard=false]
     * @return {Number|null}
     */

  }, {
    key: 'timeUntilTimeout',
    value: function timeUntilTimeout() {
      var hard = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;

      if (this.hasResponded) return null;

      var delta = void 0;
      if (hard) {
        delta = this.receivedOn + this.maxMsgTimeout - Date.now();
      } else {
        delta = this.lastTouched + this.msgTimeout - Date.now();
      }

      if (delta > 0) {
        return delta;
      }

      return null;
    }

    /**
     * Respond with a `FINISH` event.
     */

  }, {
    key: 'finish',
    value: function finish() {
      this.respond(Message.FINISH, wire.finish(this.id));
    }

    /**
     * Requeue the message with the specified amount of delay. If backoff is
     * specifed, then the subscribed Readers will backoff.
     *
     * @param  {Number}  [delay=this.requeueDelay]
     * @param  {Boolean} [backoff=true]            [description]
     */

  }, {
    key: 'requeue',
    value: function requeue() {
      var delay = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : this.requeueDelay;
      var backoff = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : true;

      this.respond(Message.REQUEUE, wire.requeue(this.id, delay));
      if (backoff) {
        this.emit(Message.BACKOFF);
      }
    }

    /**
     * Emit a `TOUCH` command. `TOUCH` command can be used to reset the timer
     * on the nsqd side. This can be done repeatedly until the message
     * is either FIN or REQ, up to the sending nsqd’s configured max_msg_timeout.
     */

  }, {
    key: 'touch',
    value: function touch() {
      this.touchCount += 1;
      this.lastTouched = Date.now();
      this.respond(Message.TOUCH, wire.touch(this.id));
    }

    /**
     * Emit a `RESPOND` event.
     *
     * @param  {String} responseType
     * @param  {String} wireData
     * @return {undefined}
     */

  }, {
    key: 'respond',
    value: function respond(responseType, wireData) {
      var _this3 = this;

      // TODO: Add a debug/warn when we moved to debug.js
      if (this.hasResponded) return;

      process.nextTick(function () {
        if (responseType !== Message.TOUCH) {
          _this3.hasResponded = true;
          clearTimeout(_this3.trackTimeoutId);
          _this3.trackTimeoutId = null;
        } else {
          _this3.lastTouched = Date.now();
        }

        _this3.emit(Message.RESPOND, responseType, wireData);
      });
    }
  }]);

  return Message;
}(EventEmitter);

module.exports = Message;