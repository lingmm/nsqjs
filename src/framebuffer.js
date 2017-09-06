'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var _ = require('lodash');

/**
 * From the NSQ protocol documentation:
 *   http://bitly.github.io/nsq/clients/tcp_protocol_spec.html
 *
 * The Frame format:
 *
 *   [x][x][x][x][x][x][x][x][x][x][x][x]...
 *   | (int32) ||  (int32) || (binary)
 *   |  4-byte  ||  4-byte  || N-byte
 *   ------------------------------------...
 *       size      frame ID     data
 */

var FrameBuffer = function () {
  function FrameBuffer() {
    _classCallCheck(this, FrameBuffer);
  }

  _createClass(FrameBuffer, [{
    key: 'consume',

    /**
     * Consume a raw message into the buffer.
     *
     * @param  {String} raw
     */
    value: function consume(raw) {
      this.buffer = Buffer.concat(_.compact([this.buffer, raw]));
    }

    /**
     * Advance the buffer and return the current slice.
     *
     * @return {String}
     */

  }, {
    key: 'nextFrame',
    value: function nextFrame() {
      if (!this.buffer) return;

      if (!this.frameSize(0) || !(this.frameSize(0) <= this.buffer.length)) {
        return;
      }

      var frame = this.pluckFrame();
      var nextOffset = this.nextOffset();
      this.buffer = this.buffer.slice(nextOffset);

      if (!this.buffer.length) {
        delete this.buffer;
      }

      return frame;
    }

    /**
     * Given an offset into a buffer, get the frame ID and data tuple.
     *
     * @param  {Number} [offset=0]
     * @return {Array}
     */

  }, {
    key: 'pluckFrame',
    value: function pluckFrame() {
      var offset = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : 0;

      var frame = this.buffer.slice(offset, offset + this.frameSize(offset));
      var frameId = frame.readInt32BE(4);
      return [frameId, frame.slice(8)];
    }

    /**
     * Given the offset of the current frame in the buffer, find the offset
     * of the next buffer.
     *
     * @param {Number} [offset=0]
     * @return {Number}
     */

  }, {
    key: 'nextOffset',
    value: function nextOffset() {
      var offset = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : 0;

      var size = this.frameSize(offset);
      if (size) {
        return offset + size;
      }
    }

    /**
     * Given the frame offset, return the frame size.
     *
     * @param {Number} offset
     * @return {Number}
     */

  }, {
    key: 'frameSize',
    value: function frameSize(offset) {
      if (!this.buffer || !(this.buffer.length > 4)) return;

      if (offset + 4 <= this.buffer.length) {
        return 4 + this.buffer.readInt32BE(offset);
      }
    }
  }]);

  return FrameBuffer;
}();

module.exports = FrameBuffer;