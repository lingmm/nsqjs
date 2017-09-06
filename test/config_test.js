'use strict';

var _require = require('../src/config'),
    ConnectionConfig = _require.ConnectionConfig,
    ReaderConfig = _require.ReaderConfig;

describe('ConnectionConfig', function () {
  var config = null;

  beforeEach(function () {
    config = new ConnectionConfig();
  });

  it('should use all defaults if nothing is provided', function () {
    config.maxInFlight.should.eql(1);
  });

  it('should validate with defaults', function () {
    var check = function check() {
      return config.validate();
    };
    check.should.not.throw();
  });

  it('should remove an unrecognized option', function () {
    config = new ConnectionConfig({ unknownOption: 20 });
    config.should.not.have.property('unknownOption');
  });

  describe('isNonEmptyString', function () {
    it('should correctly validate a non-empty string', function () {
      var check = function check() {
        return config.isNonEmptyString('name', 'worker');
      };
      check.should.not.throw();
    });

    it('should throw on an empty string', function () {
      var check = function check() {
        return config.isNonEmptyString('name', '');
      };
      check.should.throw();
    });

    it('should throw on a non-string', function () {
      var check = function check() {
        return config.isNonEmptyString('name', {});
      };
      check.should.throw();
    });
  });

  describe('isNumber', function () {
    it('should validate with a value equal to the lower bound', function () {
      var check = function check() {
        return config.isNumber('maxInFlight', 1, 1);
      };
      check.should.not.throw();
    });

    it('should validate with a value between the lower and upper bound', function () {
      var check = function check() {
        return config.isNumber('maxInFlight', 5, 1, 10);
      };
      check.should.not.throw();
    });

    it('should validate with a value equal to the upper bound', function () {
      var check = function check() {
        return config.isNumber('maxInFlight', 10, 1, 10);
      };
      check.should.not.throw();
    });

    it('should not validate with a value less than the lower bound', function () {
      var check = function check() {
        return config.isNumber('maxInFlight', -1, 1);
      };
      check.should.throw();
    });

    it('should not validate with a value greater than the upper bound', function () {
      var check = function check() {
        return config.isNumber('maxInFlight', 11, 1, 10);
      };
      check.should.throw();
    });

    it('should not validate against a non-number', function () {
      var check = function check() {
        return config.isNumber('maxInFlight', null, 0);
      };
      check.should.throw();
    });
  });

  describe('isNumberExclusive', function () {
    it('should not validate with a value equal to the lower bound', function () {
      var check = function check() {
        return config.isNumberExclusive('maxInFlight', 1, 1);
      };
      check.should.throw();
    });

    it('should validate with a value between the lower and upper bound', function () {
      var check = function check() {
        return config.isNumberExclusive('maxInFlight', 5, 1, 10);
      };
      check.should.not.throw();
    });

    it('should not validate with a value equal to the upper bound', function () {
      var check = function check() {
        return config.isNumberExclusive('maxInFlight', 10, 1, 10);
      };
      check.should.throw();
    });

    it('should not validate with a value less than the lower bound', function () {
      var check = function check() {
        return config.isNumberExclusive('maxInFlight', -1, 1);
      };
      check.should.throw();
    });

    it('should not validate with a value greater than the upper bound', function () {
      var check = function check() {
        return config.isNumberExclusive('maxInFlight', 11, 1, 10);
      };
      check.should.throw();
    });

    it('should not validate against a non-number', function () {
      var check = function check() {
        return config.isNumberExclusive('maxInFlight', null, 0);
      };
      check.should.throw();
    });
  });

  describe('isBoolean', function () {
    it('should validate against true', function () {
      var check = function check() {
        return config.isBoolean('tls', true);
      };
      check.should.not.throw();
    });

    it('should validate against false', function () {
      var check = function check() {
        return config.isBoolean('tls', false);
      };
      check.should.not.throw();
    });

    it('should not validate against null', function () {
      var check = function check() {
        return config.isBoolean('tls', null);
      };
      check.should.throw();
    });

    it('should not validate against a non-boolean value', function () {
      var check = function check() {
        return config.isBoolean('tls', 'hi');
      };
      check.should.throw();
    });
  });

  describe('isBuffer', function () {
    it('should require tls keys to be buffers', function () {
      var check = function check() {
        return config.isBuffer('key', new Buffer('a buffer'));
      };
      check.should.not.throw();
    });

    it('should require tls keys to be buffers', function () {
      var check = function check() {
        return config.isBuffer('key', 'not a buffer');
      };
      check.should.throw();
    });

    it('should require tls certs to be buffers', function () {
      var check = function check() {
        return config.isBuffer('cert', new Buffer('definitely a buffer'));
      };
      check.should.not.throw();
    });

    it('should throw when a tls cert is not a buffer', function () {
      var check = function check() {
        return config.isBuffer('cert', 'still not a buffer');
      };
      check.should.throw();
    });
  });

  describe('isArray', function () {
    it('should require cert authority chains to be arrays', function () {
      var check = function check() {
        return config.isArray('ca', ['cat', 'dog']);
      };
      check.should.not.throw();
    });

    it('should require cert authority chains to be arrays', function () {
      var check = function check() {
        return config.isArray('ca', 'not an array');
      };
      check.should.throw();
    });
  });

  describe('isBareAddresses', function () {
    it('should validate against a validate address list of 1', function () {
      var check = function check() {
        return config.isBareAddresses('nsqdTCPAddresses', ['127.0.0.1:4150']);
      };
      check.should.not.throw();
    });

    it('should validate against a validate address list of 2', function () {
      var check = function check() {
        var addrs = ['127.0.0.1:4150', 'localhost:4150'];
        config.isBareAddresses('nsqdTCPAddresses', addrs);
      };
      check.should.not.throw();
    });

    it('should not validate non-numeric port', function () {
      var check = function check() {
        return config.isBareAddresses('nsqdTCPAddresses', ['localhost']);
      };
      check.should.throw();
    });
  });

  describe('isLookupdHTTPAddresses', function () {
    it('should validate against a validate address list of 1', function () {
      var check = function check() {
        return config.isLookupdHTTPAddresses('lookupdHTTPAddresses', ['127.0.0.1:4150']);
      };
      check.should.not.throw();
    });

    it('should validate against a validate address list of 2', function () {
      var check = function check() {
        var addrs = ['127.0.0.1:4150', 'localhost:4150', 'http://localhost/nsq/lookup', 'https://localhost/nsq/lookup'];
        config.isLookupdHTTPAddresses('lookupdHTTPAddresses', addrs);
      };
      check.should.not.throw();
    });

    it('should not validate non-numeric port', function () {
      var check = function check() {
        return config.isLookupdHTTPAddresses('lookupdHTTPAddresses', ['localhost']);
      };
      check.should.throw();
    });

    it('should not validate non-HTTP/HTTPs address', function () {
      var check = function check() {
        return config.isLookupdHTTPAddresses('lookupdHTTPAddresses', ['localhost']);
      };
      check.should.throw();
    });
  });
});

describe('ReaderConfig', function () {
  var config = null;

  beforeEach(function () {
    config = new ReaderConfig();
  });

  it('should use all defaults if nothing is provided', function () {
    config.maxInFlight.should.eql(1);
  });

  it('should validate with defaults', function () {
    var check = function check() {
      config = new ReaderConfig({ nsqdTCPAddresses: ['127.0.0.1:4150'] });
      config.validate();
    };
    check.should.not.throw();
  });

  it('should convert a string address to an array', function () {
    config = new ReaderConfig({ lookupdHTTPAddresses: '127.0.0.1:4161' });
    config.validate();
    config.lookupdHTTPAddresses.length.should.equal(1);
  });
});