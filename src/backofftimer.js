'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var _require = require('bignumber.js'),
    BigNumber = _require.BigNumber;

var min = function min(a, b) {
  return a.lte(b) ? a : b;
};
var max = function max(a, b) {
  return a.gte(b) ? a : b;
};

/**
 * This is a timer that is smart about backing off exponentially
 * when there are problems.
 *
 * Ported from pynsq:
 *   https://github.com/bitly/pynsq/blob/master/nsq/BackoffTimer.py
 */

var BackoffTimer = function () {
  /**
   * Instantiates a new instance of BackoffTimer.
   *
   * @constructor
   * @param  {Number} minInterval
   * @param  {Number} maxInterval
   * @param  {Number} [ratio=0.25]
   * @param  {Number} [shortLength=10]
   * @param  {Number} [longLength=250]
   */
  function BackoffTimer(minInterval, maxInterval) {
    var ratio = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : 0.25;
    var shortLength = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : 10;
    var longLength = arguments.length > 4 && arguments[4] !== undefined ? arguments[4] : 250;

    _classCallCheck(this, BackoffTimer);

    this.minInterval = new BigNumber(minInterval);
    this.maxInterval = new BigNumber(maxInterval);

    ratio = new BigNumber(ratio);
    var intervalDelta = new BigNumber(this.maxInterval - this.minInterval);

    // (maxInterval - minInterval) * ratio
    this.maxShortTimer = intervalDelta.times(ratio);

    // (maxInterval - minInterval) * (1 - ratio)
    this.maxLongTimer = intervalDelta.times(new BigNumber(1).minus(ratio));

    this.shortUnit = this.maxShortTimer.dividedBy(shortLength);
    this.longUnit = this.maxLongTimer.dividedBy(longLength);

    this.shortInterval = new BigNumber(0);
    this.longInterval = new BigNumber(0);
  }

  /**
   * On success updates the backoff timers.
   */


  _createClass(BackoffTimer, [{
    key: 'success',
    value: function success() {
      this.shortInterval = this.shortInterval.minus(this.shortUnit);
      this.longInterval = this.longInterval.minus(this.longUnit);
      this.shortInterval = max(this.shortInterval, new BigNumber(0));
      this.longInterval = max(this.longInterval, new BigNumber(0));
    }

    /**
     * On failure updates the backoff timers.
     */

  }, {
    key: 'failure',
    value: function failure() {
      this.shortInterval = this.shortInterval.plus(this.shortUnit);
      this.longInterval = this.longInterval.plus(this.longUnit);
      this.shortInterval = min(this.shortInterval, this.maxShortTimer);
      this.longInterval = min(this.longInterval, this.maxLongTimer);
    }

    /**
     * Get the next backoff interval.
     *
     * @return {Number}
     */

  }, {
    key: 'getInterval',
    value: function getInterval() {
      return this.minInterval.plus(this.shortInterval.plus(this.longInterval));
    }
  }]);

  return BackoffTimer;
}();

module.exports = BackoffTimer;