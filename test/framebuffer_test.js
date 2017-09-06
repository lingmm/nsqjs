'use strict';

var _slicedToArray = function () { function sliceIterator(arr, i) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"]) _i["return"](); } finally { if (_d) throw _e; } } return _arr; } return function (arr, i) { if (Array.isArray(arr)) { return arr; } else if (Symbol.iterator in Object(arr)) { return sliceIterator(arr, i); } else { throw new TypeError("Invalid attempt to destructure non-iterable instance"); } }; }();

var should = require('should');

var wire = require('../src/wire');
var FrameBuffer = require('../src/framebuffer');

var createFrame = function createFrame(frameId, payload) {
  var frame = new Buffer(4 + 4 + payload.length);
  frame.writeInt32BE(payload.length + 4, 0);
  frame.writeInt32BE(frameId, 4);
  frame.write(payload, 8);
  return frame;
};

describe('FrameBuffer', function () {
  it('should parse a single, full frame', function () {
    var frameBuffer = new FrameBuffer();
    var data = createFrame(wire.FRAME_TYPE_RESPONSE, 'OK');
    frameBuffer.consume(data);

    var _Array$from = Array.from(frameBuffer.nextFrame()),
        _Array$from2 = _slicedToArray(_Array$from, 2),
        frameId = _Array$from2[0],
        payload = _Array$from2[1];

    frameId.should.eql(wire.FRAME_TYPE_RESPONSE);
    payload.toString().should.eql('OK');
  });

  it('should parse two full frames', function () {
    var frameBuffer = new FrameBuffer();

    var firstFrame = createFrame(wire.FRAME_TYPE_RESPONSE, 'OK');
    var secondFrame = createFrame(wire.FRAME_TYPE_ERROR, JSON.stringify({ shortname: 'localhost' }));

    frameBuffer.consume(Buffer.concat([firstFrame, secondFrame]));
    var frames = [frameBuffer.nextFrame(), frameBuffer.nextFrame()];
    frames.length.should.eql(2);

    var _Array$from3 = Array.from(frames.shift()),
        _Array$from4 = _slicedToArray(_Array$from3, 2),
        frameId = _Array$from4[0],
        data = _Array$from4[1];

    frameId.should.eql(wire.FRAME_TYPE_RESPONSE);
    data.toString().should.eql('OK');
    var _Array$from5 = Array.from(frames.shift());

    var _Array$from6 = _slicedToArray(_Array$from5, 2);

    frameId = _Array$from6[0];
    data = _Array$from6[1];

    frameId.should.eql(wire.FRAME_TYPE_ERROR);
    data.toString().should.eql(JSON.stringify({ shortname: 'localhost' }));
  });

  it('should parse frame delivered in partials', function () {
    var frameBuffer = new FrameBuffer();
    var data = createFrame(wire.FRAME_TYPE_RESPONSE, 'OK');

    // First frame is 10 bytes long. Don't expect to get anything back.
    frameBuffer.consume(data.slice(0, 3));
    should.not.exist(frameBuffer.nextFrame());

    // Yup, still haven't received the whole frame.
    frameBuffer.consume(data.slice(3, 8));
    should.not.exist(frameBuffer.nextFrame());

    // Got the whole first frame.
    frameBuffer.consume(data.slice(8));
    should.exist(frameBuffer.nextFrame());
  });

  it('should parse multiple frames delivered in partials', function () {
    var frameBuffer = new FrameBuffer();
    var first = createFrame(wire.FRAME_TYPE_RESPONSE, 'OK');
    var second = createFrame(wire.FRAME_TYPE_RESPONSE, '{}');
    var data = Buffer.concat([first, second]);

    // First frame is 10 bytes long. Don't expect to get anything back.
    frameBuffer.consume(data.slice(0, 3));
    should.not.exist(frameBuffer.nextFrame());

    // Yup, still haven't received the whole frame.
    frameBuffer.consume(data.slice(3, 8));
    should.not.exist(frameBuffer.nextFrame());

    // Got the whole first frame and part of the 2nd frame.
    frameBuffer.consume(data.slice(8, 12));
    should.exist(frameBuffer.nextFrame());

    // Got the 2nd frame.
    frameBuffer.consume(data.slice(12));
    should.exist(frameBuffer.nextFrame());
  });

  return it('empty internal buffer when all frames are consumed', function () {
    var frameBuffer = new FrameBuffer();
    var data = createFrame(wire.FRAME_TYPE_RESPONSE, 'OK');

    frameBuffer.consume(data);
    while (frameBuffer.nextFrame()) {}

    should.not.exist(frameBuffer.buffer);
  });
});