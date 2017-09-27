'use strict';

var _get = function get(object, property, receiver) { if (object === null) object = Function.prototype; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { return get(parent, property, receiver); } } else if ("value" in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } };

var _slicedToArray = function () { function sliceIterator(arr, i) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"]) _i["return"](); } finally { if (_d) throw _e; } } return _arr; } return function (arr, i) { if (Array.isArray(arr)) { return arr; } else if (Symbol.iterator in Object(arr)) { return sliceIterator(arr, i); } else { throw new TypeError("Invalid attempt to destructure non-iterable instance"); } }; }();

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

function _toConsumableArray(arr) { if (Array.isArray(arr)) { for (var i = 0, arr2 = Array(arr.length); i < arr.length; i++) { arr2[i] = arr[i]; } return arr2; } else { return Array.from(arr); } }

function _toArray(arr) { return Array.isArray(arr) ? arr : Array.from(arr); }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var _ = require('lodash');
var url = require('url');

/**
 * Responsible for configuring the official defaults for nsqd connections.
 * @type {ConnectionConfig}
 */

var ConnectionConfig = function () {
  _createClass(ConnectionConfig, null, [{
    key: 'isBareAddress',


    /**
     * Indicates if an address has the host pair combo.
     *
     * @param  {String}  addr
     * @return {Boolean}
     */
    value: function isBareAddress(addr) {
      var _addr$split = addr.split(':'),
          _addr$split2 = _slicedToArray(_addr$split, 2),
          host = _addr$split2[0],
          port = _addr$split2[1];

      return host.length > 0 && port > 0;
    }

    /**
     * Instantiates a new ConnectionConfig.
     *
     * @constructor
     * @param  {Object} [options={}]
     */

  }, {
    key: 'DEFAULTS',
    get: function get() {
      return {
        authSecret: null,
        clientId: null,
        deflate: false,
        deflateLevel: 6,
        heartbeatInterval: 30,
        maxInFlight: 1,
        messageTimeout: null,
        outputBufferSize: null,
        outputBufferTimeout: null,
        requeueDelay: 90,
        sampleRate: null,
        snappy: false,
        tls: false,
        tlsVerification: true,
        key: null,
        cert: null,
        ca: null,
        idleTimeout: 0,
        maxReconnect: 5
      };
    }
  }]);

  function ConnectionConfig() {
    var options = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};

    _classCallCheck(this, ConnectionConfig);

    options = _.chain(options).pick(_.keys(this.constructor.DEFAULTS)).defaults(this.constructor.DEFAULTS).value();

    _.extend(this, options);
  }

  /**
   * Throws an error if the value is not a non empty string.
   *
   * @param  {String}  option
   * @param  {*}  value
   */


  _createClass(ConnectionConfig, [{
    key: 'isNonEmptyString',
    value: function isNonEmptyString(option, value) {
      if (!_.isString(value) || !(value.length > 0)) {
        throw new Error(option + ' must be a non-empty string');
      }
    }

    /**
     * Throws an error if the value is not a number.
     *
     * @param  {String}  option
     * @param  {*}  value
     * @param  {*}  lower
     * @param  {*}  upper
     */

  }, {
    key: 'isNumber',
    value: function isNumber(option, value, lower, upper) {
      if (_.isNaN(value) || !_.isNumber(value)) {
        throw new Error(option + '(' + value + ') is not a number');
      }

      if (upper) {
        if (!(lower <= value && value <= upper)) {
          throw new Error(lower + ' <= ' + option + '(' + value + ') <= ' + upper);
        }
      } else if (!(lower <= value)) {
        throw new Error(lower + ' <= ' + option + '(' + value + ')');
      }
    }

    /**
     * Throws an error if the value is not exclusive.
     *
     * @param  {String}  option
     * @param  {*}  value
     * @param  {*}  lower
     * @param  {*}  upper
     */

  }, {
    key: 'isNumberExclusive',
    value: function isNumberExclusive(option, value, lower, upper) {
      if (_.isNaN(value) || !_.isNumber(value)) {
        throw new Error(option + '(' + value + ') is not a number');
      }

      if (upper) {
        if (!(lower < value && value < upper)) {
          throw new Error(lower + ' < ' + option + '(' + value + ') < ' + upper);
        }
      } else if (!(lower < value)) {
        throw new Error(lower + ' < ' + option + '(' + value + ')');
      }
    }

    /**
     * Throws an error if the option is not a Boolean.
     *
     * @param  {String}  option
     * @param  {*}  value
     */

  }, {
    key: 'isBoolean',
    value: function isBoolean(option, value) {
      if (!_.isBoolean(value)) {
        throw new Error(option + ' must be either true or false');
      }
    }

    /**
     * Throws an error if the option is not a bare address.
     *
     * @param  {String}  option
     * @param  {*}  value
     */

  }, {
    key: 'isBareAddresses',
    value: function isBareAddresses(option, value) {
      if (!_.isArray(value) || !_.every(value, ConnectionConfig.isBareAddress)) {
        throw new Error(option + ' must be a list of addresses \'host:port\'');
      }
    }

    /**
     * Throws an error if the option is not a valid lookupd http address.
     *
     * @param  {String}  option
     * @param  {*}  value
     */

  }, {
    key: 'isLookupdHTTPAddresses',
    value: function isLookupdHTTPAddresses(option, value) {
      var isAddr = function isAddr(addr) {
        if (addr.indexOf('://') === -1) {
          return ConnectionConfig.isBareAddress(addr);
        }

        var parsedUrl = url.parse(addr);
        return ['http:', 'https:'].includes(parsedUrl.protocol) && !!parsedUrl.host;
      };

      if (!_.isArray(value) || !_.every(value, isAddr)) {
        throw new Error(option + ' must be a list of addresses \'host:port\' or HTTP/HTTPS URI');
      }
    }

    /**
     * Throws an error if the option is not a buffer.
     *
     * @param  {String}  option
     * @param  {*}  value
     */

  }, {
    key: 'isBuffer',
    value: function isBuffer(option, value) {
      if (!Buffer.isBuffer(value)) {
        throw new Error(option + ' must be a buffer');
      }
    }

    /**
     * Throws an error if the option is not an array.
     *
     * @param  {String}  option
     * @param  {*}  value
     */

  }, {
    key: 'isArray',
    value: function isArray(option, value) {
      if (!_.isArray(value)) {
        throw new Error(option + ' must be an array');
      }
    }

    /**
     * Returns the validated client config. Throws an error if any values are
     * not correct.
     *
     * @return {Object}
     */

  }, {
    key: 'conditions',
    value: function conditions() {
      return {
        authSecret: [this.isNonEmptyString],
        clientId: [this.isNonEmptyString],
        deflate: [this.isBoolean],
        deflateLevel: [this.isNumber, 0, 9],
        heartbeatInterval: [this.isNumber, 1],
        maxInFlight: [this.isNumber, 1],
        messageTimeout: [this.isNumber, 1],
        outputBufferSize: [this.isNumber, 64],
        outputBufferTimeout: [this.isNumber, 1],
        requeueDelay: [this.isNumber, 0],
        sampleRate: [this.isNumber, 1, 99],
        snappy: [this.isBoolean],
        tls: [this.isBoolean],
        tlsVerification: [this.isBoolean],
        key: [this.isBuffer],
        cert: [this.isBuffer],
        ca: [this.isArray],
        idleTimeout: [this.isNumber, 0]
      };
    }

    /**
     * Helper function that will validate a condition with the given args.
     *
     * @param  {String} option
     * @param  {String} value
     * @return {Boolean}
     */

  }, {
    key: 'validateOption',
    value: function validateOption(option, value) {
      var _conditions$option = _toArray(this.conditions()[option]),
          fn = _conditions$option[0],
          args = _conditions$option.slice(1);

      return fn.apply(undefined, [option, value].concat(_toConsumableArray(args)));
    }

    /**
     * Validate the connection options.
     */

  }, {
    key: 'validate',
    value: function validate() {
      var options = Object.keys(this);
      var _iteratorNormalCompletion = true;
      var _didIteratorError = false;
      var _iteratorError = undefined;

      try {
        for (var _iterator = options[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
          var option = _step.value;

          // dont validate our methods
          var value = this[option];

          if (_.isFunction(value)) {
            continue;
          }

          // Skip options that default to null
          if (_.isNull(value) && this.constructor.DEFAULTS[option] === null) {
            continue;
          }

          // Disabled via -1
          var keys = ['outputBufferSize', 'outputBufferTimeout'];
          if (keys.includes(option) && value === -1) {
            continue;
          }

          this.validateOption(option, value);
        }

        // Mutually exclusive options
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

      if (this.snappy && this.deflate) {
        throw new Error('Cannot use both deflate and snappy');
      }

      if (this.snappy) {
        try {
          require('snappystream');
        } catch (err) {
          throw new Error('Cannot use snappy since it did not successfully install via npm.');
        }
      }
    }
  }]);

  return ConnectionConfig;
}();

/**
 * Responsible for configuring the official defaults for Reader connections.
 * @type {[type]}
 */


var ReaderConfig = function (_ConnectionConfig) {
  _inherits(ReaderConfig, _ConnectionConfig);

  function ReaderConfig() {
    _classCallCheck(this, ReaderConfig);

    return _possibleConstructorReturn(this, (ReaderConfig.__proto__ || Object.getPrototypeOf(ReaderConfig)).apply(this, arguments));
  }

  _createClass(ReaderConfig, [{
    key: 'conditions',


    /**
     * Returns the validated reader client config. Throws an error if any
     * values are not correct.
     *
     * @return {Object}
     */
    value: function conditions() {
      return _.extend({}, _get(ReaderConfig.prototype.__proto__ || Object.getPrototypeOf(ReaderConfig.prototype), 'conditions', this).call(this), {
        lookupdHTTPAddresses: [this.isLookupdHTTPAddresses],
        lookupdPollInterval: [this.isNumber, 1],
        lookupdPollJitter: [this.isNumberExclusive, 0, 1],
        name: [this.isNonEmptyString],
        nsqdTCPAddresses: [this.isBareAddresses],
        maxAttempts: [this.isNumber, 0],
        maxBackoffDuration: [this.isNumber, 0]
      });
    }

    /**
     * Validate the connection options.
     */

  }, {
    key: 'validate',
    value: function validate() {
      var _get2,
          _this2 = this;

      var addresses = ['nsqdTCPAddresses', 'lookupdHTTPAddresses'];

      /**
       * Either a string or list of strings can be provided. Ensure list of
       * strings going forward.
       */
      var _iteratorNormalCompletion2 = true;
      var _didIteratorError2 = false;
      var _iteratorError2 = undefined;

      try {
        for (var _iterator2 = Array.from(addresses)[Symbol.iterator](), _step2; !(_iteratorNormalCompletion2 = (_step2 = _iterator2.next()).done); _iteratorNormalCompletion2 = true) {
          var key = _step2.value;

          if (_.isString(this[key])) {
            this[key] = [this[key]];
          }
        }
      } catch (err) {
        _didIteratorError2 = true;
        _iteratorError2 = err;
      } finally {
        try {
          if (!_iteratorNormalCompletion2 && _iterator2.return) {
            _iterator2.return();
          }
        } finally {
          if (_didIteratorError2) {
            throw _iteratorError2;
          }
        }
      }

      for (var _len = arguments.length, args = Array(_len), _key = 0; _key < _len; _key++) {
        args[_key] = arguments[_key];
      }

      (_get2 = _get(ReaderConfig.prototype.__proto__ || Object.getPrototypeOf(ReaderConfig.prototype), 'validate', this)).call.apply(_get2, [this].concat(args));

      var pass = _.chain(addresses).map(function (key) {
        return _this2[key].length;
      }).some(_.identity).value();

      if (!pass) {
        throw new Error('Need to provide either ' + addresses.join(' or '));
      }
    }
  }], [{
    key: 'DEFAULTS',
    get: function get() {
      return _.extend({}, ConnectionConfig.DEFAULTS, {
        lookupdHTTPAddresses: [],
        lookupdPollInterval: 60,
        lookupdPollJitter: 0.3,
        name: null,
        nsqdTCPAddresses: [],
        maxAttempts: 0,
        maxBackoffDuration: 128
      });
    }
  }]);

  return ReaderConfig;
}(ConnectionConfig);

module.exports = { ConnectionConfig: ConnectionConfig, ReaderConfig: ReaderConfig };