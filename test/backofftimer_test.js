'use strict';

var BackoffTimer = require('../src/backofftimer');

describe('backofftimer', function () {
  var timer = null;
  beforeEach(function () {
    timer = new BackoffTimer(0, 128);
  });

  describe('constructor', function () {
    it('should have @maxShortTimer eq 1', function () {
      timer.maxShortTimer.toString().should.eql('32');
    });

    it('should have a @maxLongTimer eq 3', function () {
      timer.maxLongTimer.toString().should.eql('96');
    });

    it('should have a @shortUnit equal to 0.1', function () {
      timer.shortUnit.toString().should.eql('3.2');
    });

    it('should have a @longUnit equal to 0.3', function () {
      timer.longUnit.toString().should.eql('0.384');
    });
  });

  describe('success', function () {
    it('should adjust @shortInterval to 0', function () {
      timer.success();
      timer.shortInterval.toString().should.eql('0');
    });

    it('should adjust @longInterval to 0', function () {
      timer.success();
      timer.longInterval.toString().should.eql('0');
    });
  });

  describe('failure', function () {
    it('should adjust @shortInterval to 3.2 after 1 failure', function () {
      timer.failure();
      timer.shortInterval.toString().should.eql('3.2');
    });

    it('should adjust @longInterval to .384 after 1 failure', function () {
      timer.failure();
      timer.longInterval.toString().should.eql('0.384');
    });
  });

  describe('getInterval', function () {
    it('should initially be 0', function () {
      timer.getInterval().toString().should.eql('0');
    });

    it('should be 0 after 1 success', function () {
      timer.success();
      timer.getInterval().toString().should.eql('0');
    });

    it('should be 0 after 2 successes', function () {
      timer.success();
      timer.success();
      timer.getInterval().toString().should.eql('0');
    });

    it('should be 3.584 after 1 failure', function () {
      timer.failure();
      timer.getInterval().toString().should.eql('3.584');
    });

    it('should be 7.168 after 2 failure', function () {
      timer.failure();
      timer.failure();
      timer.getInterval().toString().should.eql('7.168');
    });
  });
});