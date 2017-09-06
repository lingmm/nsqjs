'use strict';

var assert = require('assert');

var _ = require('lodash');
var should = require('should');

var RoundRobinList = require('../src/roundrobinlist');

describe('roundrobinlist', function () {
  var list = null;
  var rrl = null;

  beforeEach(function () {
    list = [1, 2, 3];
    rrl = new RoundRobinList(list);
  });

  describe('constructor', function () {
    it('should have @list eq to passed in list', function () {
      return assert(_.isEqual(rrl.list, list));
    });

    it('should have made a copy of the list argument', function () {
      return assert(rrl.list !== list);
    });

    it('should have @index eq to 0', function () {
      return rrl.index.should.eql(0);
    });
  });

  describe('add', function () {
    return it('@list should include the item', function () {
      rrl.add(10);
      should.ok(Array.from(rrl.list).includes(10));
    });
  });

  describe('next', function () {
    it('should return a list of 1 item by default', function () {
      assert(_.isEqual(rrl.next(), list.slice(0, 1)));
      rrl.index.should.eql(1);
    });

    it('should return a list as large as the count provided', function () {
      assert(_.isEqual(rrl.next(2), list.slice(0, 2)));
      rrl.index.should.eql(2);
    });

    it('should return all items and and then start over', function () {
      assert(_.isEqual(rrl.next(), [1]));
      assert(_.isEqual(rrl.next(), [2]));
      assert(_.isEqual(rrl.next(), [3]));
      assert(_.isEqual(rrl.next(), [1]));
    });
  });

  describe('remove', function () {
    it('should remove the item if it exists in the list', function () {
      rrl.remove(3);
      should.ok(!Array.from(rrl.list).includes(3));
    });

    it('should not affect the order of items returned', function () {
      rrl.remove(1);
      assert(_.isEqual(rrl.next(), [2]));
      assert(_.isEqual(rrl.next(), [3]));
      assert(_.isEqual(rrl.next(), [2]));
    });

    it('should not affect the order of items returned with items consumed', function () {
      assert(_.isEqual(rrl.next(), [1]));
      assert(_.isEqual(rrl.next(), [2]));
      rrl.remove(2);
      assert(_.isEqual(rrl.next(), [3]));
      assert(_.isEqual(rrl.next(), [1]));
    });

    it('should silently fail when it does not have the item', function () {
      rrl.remove(10);
      assert(_.isEqual(rrl.list, [1, 2, 3]));
      rrl.index.should.eql(0);
    });
  });
});