'use strict';

var _ = require('lodash');
var nock = require('nock');
var should = require('should');

var lookup = require('../src/lookupd');

var NSQD_1 = {
  address: 'localhost',
  broadcast_address: 'localhost',
  hostname: 'localhost',
  http_port: 4151,
  remote_address: 'localhost:12345',
  tcp_port: 4150,
  topics: ['sample_topic'],
  version: '0.2.23'
};
var NSQD_2 = {
  address: 'localhost',
  broadcast_address: 'localhost',
  hostname: 'localhost',
  http_port: 5151,
  remote_address: 'localhost:56789',
  tcp_port: 5150,
  topics: ['sample_topic'],
  version: '0.2.23'
};
var NSQD_3 = {
  address: 'localhost',
  broadcast_address: 'localhost',
  hostname: 'localhost',
  http_port: 6151,
  remote_address: 'localhost:23456',
  tcp_port: 6150,
  topics: ['sample_topic'],
  version: '0.2.23'
};
var NSQD_4 = {
  address: 'localhost',
  broadcast_address: 'localhost',
  hostname: 'localhost',
  http_port: 7151,
  remote_address: 'localhost:34567',
  tcp_port: 7150,
  topics: ['sample_topic'],
  version: '0.2.23'
};

var LOOKUPD_1 = '127.0.0.1:4161';
var LOOKUPD_2 = '127.0.0.1:5161';
var LOOKUPD_3 = 'http://127.0.0.1:6161/';
var LOOKUPD_4 = 'http://127.0.0.1:7161/path/lookup';

var nockUrlSplit = function nockUrlSplit(url) {
  var match = url.match(/^(https?:\/\/[^/]+)(\/.*$)/i);
  return {
    baseUrl: match[1],
    path: match[2]
  };
};

var registerWithLookupd = function registerWithLookupd(lookupdAddress, nsqd) {
  var producers = nsqd != null ? [nsqd] : [];

  if (nsqd != null) {
    nsqd.topics.forEach(function (topic) {
      if (lookupdAddress.indexOf('://') === -1) {
        nock('http://' + lookupdAddress).get('/lookup?topic=' + topic).reply(200, {
          status_code: 200,
          status_txt: 'OK',
          producers: producers
        });
      } else {
        var params = nockUrlSplit(lookupdAddress);
        var baseUrl = params.baseUrl;
        var path = params.path;

        if (!path || path === '/') {
          path = '/lookup';
        }

        nock(baseUrl).get(path + '?topic=' + topic).reply(200, {
          status_code: 200,
          status_txt: 'OK',
          producers: producers
        });
      }
    });
  }
};

var setFailedTopicReply = function setFailedTopicReply(lookupdAddress, topic) {
  return nock('http://' + lookupdAddress).get('/lookup?topic=' + topic).reply(200, {
    status_code: 404,
    status_txt: 'TOPIC_NOT_FOUND'
  });
};

describe('lookupd.lookup', function () {
  afterEach(function () {
    return nock.cleanAll();
  });

  describe('querying a single lookupd for a topic', function () {
    it('should return an empty list if no nsqd nodes', function (done) {
      setFailedTopicReply(LOOKUPD_1, 'sample_topic');

      lookup(LOOKUPD_1, 'sample_topic', function (err, nodes) {
        nodes.should.be.empty();
        done(err);
      });
    });

    it('should return a list of nsqd nodes for a success reply', function (done) {
      registerWithLookupd(LOOKUPD_1, NSQD_1);

      lookup(LOOKUPD_1, 'sample_topic', function (err, nodes) {
        nodes.should.have.length(1);['address', 'broadcast_address', 'tcp_port', 'http_port'].forEach(function (key) {
          should.ok(_.keys(nodes[0]).includes(key));
        });
        done(err);
      });
    });
  });

  describe('querying a multiple lookupd', function () {
    it('should combine results from multiple lookupds', function (done) {
      registerWithLookupd(LOOKUPD_1, NSQD_1);
      registerWithLookupd(LOOKUPD_2, NSQD_2);
      registerWithLookupd(LOOKUPD_3, NSQD_3);
      registerWithLookupd(LOOKUPD_4, NSQD_4);

      var lookupdAddresses = [LOOKUPD_1, LOOKUPD_2, LOOKUPD_3, LOOKUPD_4];
      lookup(lookupdAddresses, 'sample_topic', function (err, nodes) {
        nodes.should.have.length(4);
        _.chain(nodes).map(function (n) {
          return n['tcp_port'];
        }).sort().value().should.be.eql([4150, 5150, 6150, 7150]);
        done(err);
      });
    });

    it('should dedupe combined results', function (done) {
      registerWithLookupd(LOOKUPD_1, NSQD_1);
      registerWithLookupd(LOOKUPD_2, NSQD_1);
      registerWithLookupd(LOOKUPD_3, NSQD_1);
      registerWithLookupd(LOOKUPD_4, NSQD_1);

      var lookupdAddresses = [LOOKUPD_1, LOOKUPD_2, LOOKUPD_3, LOOKUPD_4];
      lookup(lookupdAddresses, 'sample_topic', function (err, nodes) {
        nodes.should.have.length(1);
        done(err);
      });
    });

    return it('should succeed inspite of failures to query a lookupd', function (done) {
      registerWithLookupd(LOOKUPD_1, NSQD_1);
      nock('http://' + LOOKUPD_2).get('/lookup?topic=sample_topic').reply(500);

      var lookupdAddresses = [LOOKUPD_1, LOOKUPD_2];
      lookup(lookupdAddresses, 'sample_topic', function (err, nodes) {
        nodes.should.have.length(1);
        done(err);
      });
    });
  });
});