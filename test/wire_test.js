'use strict';

var _slicedToArray = function () { function sliceIterator(arr, i) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"]) _i["return"](); } finally { if (_d) throw _e; } } return _arr; } return function (arr, i) { if (Array.isArray(arr)) { return arr; } else if (Symbol.iterator in Object(arr)) { return sliceIterator(arr, i); } else { throw new TypeError("Invalid attempt to destructure non-iterable instance"); } }; }();

function _toConsumableArray(arr) { if (Array.isArray(arr)) { for (var i = 0, arr2 = Array(arr.length); i < arr.length; i++) { arr2[i] = arr[i]; } return arr2; } else { return Array.from(arr); } }

var should = require('should');
var wire = require('../src/wire');

var matchCommand = function matchCommand(commandFn, args, expected) {
  var commandOut = commandFn.apply(undefined, _toConsumableArray(args));
  should.equal(commandOut.toString(), expected);
};

describe('nsq wire', function () {
  it('should construct an identity message', function () {
    matchCommand(wire.identify, [{ short_id: 1, long_id: 2 }], 'IDENTIFY\n\0\0\0\x1A{"short_id":1,"long_id":2}');
  });

  it('should construct an identity message with unicode', function () {
    return matchCommand(wire.identify, [{ long_id: 'w\xC3\xA5\xE2\x80\xA0' }], 'IDENTIFY\n\0\0\0-{"long_id":"w\\u00c3\\u00a5\\u00e2' + '\\u0080\\u00a0"}');
  });

  it('should subscribe to a topic and channel', function () {
    return matchCommand(wire.subscribe, ['test_topic', 'test_channel'], 'SUB test_topic test_channel\n');
  });

  it('should finish a message', function () {
    return matchCommand(wire.finish, ['test'], 'FIN test\n');
  });

  it('should finish a message with a unicode id', function () {
    return matchCommand(wire.finish, ['\xFCn\xEE\xE7\xF8\u2202\xE9'], 'FIN \xFCn\xEE\xE7\xF8\u2202\xE9\n');
  });

  it('should requeue a message', function () {
    return matchCommand(wire.requeue, ['test'], 'REQ test 0\n');
  });

  it('should requeue a message with timeout', function () {
    return matchCommand(wire.requeue, ['test', 60], 'REQ test 60\n');
  });

  it('should touch a message', function () {
    return matchCommand(wire.touch, ['test'], 'TOUCH test\n');
  });

  it('should construct a ready message', function () {
    return matchCommand(wire.ready, [100], 'RDY 100\n');
  });

  it('should construct a no-op message', function () {
    return matchCommand(wire.nop, [], 'NOP\n');
  });

  it('should publish a message', function () {
    return matchCommand(wire.pub, ['test_topic', 'abcd'], 'PUB test_topic\n\0\0\0\x04abcd');
  });

  it('should publish a multi-byte string message', function () {
    return matchCommand(wire.pub, ['test_topic', 'こんにちは'], 'PUB test_topic\n\0\0\0\x0F\u3053\u3093\u306B\u3061\u306F');
  });

  it('should publish multiple string messages', function () {
    return matchCommand(wire.mpub, ['test_topic', ['abcd', 'efgh', 'ijkl']], ['MPUB test_topic\n\0\0\0\x1C\0\0\0\x03', '\0\0\0\x04abcd', '\0\0\0\x04efgh', '\0\0\0\x04ijkl'].join(''));
  });

  it('should publish multiple buffer messages', function () {
    return matchCommand(wire.mpub, ['test_topic', [new Buffer('abcd'), new Buffer('efgh')]], ['MPUB test_topic\n\0\0\0\x14\0\0\0\x02', '\0\0\0\x04abcd', '\0\0\0\x04efgh'].join(''));
  });

  return it('should unpack a received message', function () {
    var msgPayload = ['132cb60626e9fd7a00013035356335626531636534333330323769747265616c6c7974', '696564746865726f6f6d746f676574686572'];
    var msgParts = wire.unpackMessage(new Buffer(msgPayload.join(''), 'hex'));

    var _Array$from = Array.from(msgParts),
        _Array$from2 = _slicedToArray(_Array$from, 3),
        id = _Array$from2[0],
        timestamp = _Array$from2[1],
        attempts = _Array$from2[2];

    timestamp.toString(10).should.eql('1381679323234827642');
    id.should.eql('055c5be1ce433027');
    return attempts.should.eql(1);
  });
});