"use strict";

function asyncGeneratorStep(gen, resolve, reject, _next, _throw, key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { Promise.resolve(value).then(_next, _throw); } }

function _asyncToGenerator(fn) { return function () { var self = this, args = arguments; return new Promise(function (resolve, reject) { var gen = fn.apply(self, args); function _next(value) { asyncGeneratorStep(gen, resolve, reject, _next, _throw, "next", value); } function _throw(err) { asyncGeneratorStep(gen, resolve, reject, _next, _throw, "throw", err); } _next(undefined); }); }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

var winston = require('winston');

var _require = require('connection-string-parser'),
    ConnectionStringParser = _require.ConnectionStringParser;

var _require2 = require('mongodb'),
    ReplSet = _require2.ReplSet,
    Server = _require2.Server,
    MongoClient = _require2.MongoClient;

var _require3 = require('./errors'),
    DatabaseConnectionError = _require3.DatabaseConnectionError;

var connectionStringParser = new ConnectionStringParser({
  scheme: "mongodb",
  hosts: []
});

var connector =
/*#__PURE__*/
function () {
  function connector() {
    var _this = this;

    _classCallCheck(this, connector);

    _defineProperty(this, "client", null);

    _defineProperty(this, "connected", false);

    _defineProperty(this, "connectionString", '');

    _defineProperty(this, "disconnect", function () {
      if (_this.connected) {
        _this.client.close();

        _this.connected = false;
      } else {
        throw new DatabaseConnectionError();
      }
    });
  }

  _createClass(connector, [{
    key: "replSet",
    value: function replSet(configs) {
      try {
        if (Array.isArray(configs) && configs.length > 0) {
          var servers = configs.map(function (config) {
            return new Server(config.host, parseInt(mongoConfig.port, 10));
          });
          var replSet = new ReplSet(servers);
          return replSet;
        }

        var connectionObject = connectionStringParser.parse(connector.connectionString);
        return new ReplSet(connectionObject.hosts);
      } catch (error) {
        winston.log('error', {
          error: error.message,
          stack: error.stack
        });
      }
    }
  }]);

  return connector;
}();

_defineProperty(connector, "connect",
/*#__PURE__*/
function () {
  var _ref2 = _asyncToGenerator(
  /*#__PURE__*/
  regeneratorRuntime.mark(function _callee(_ref) {
    var connectionString, dbClient;
    return regeneratorRuntime.wrap(function _callee$(_context) {
      while (1) {
        switch (_context.prev = _context.next) {
          case 0:
            connectionString = _ref.connectionString;
            _context.prev = 1;
            _context.next = 4;
            return MongoClient.connect(connectionString, {
              poolSize: 200,
              useNewUrlParser: true
            });

          case 4:
            dbClient = _context.sent;
            connector.client = dbClient;
            connector.connected = true;
            connector.connectionString = connectionString;
            return _context.abrupt("return", dbClient);

          case 11:
            _context.prev = 11;
            _context.t0 = _context["catch"](1);
            throw _context.t0;

          case 14:
          case "end":
            return _context.stop();
        }
      }
    }, _callee, null, [[1, 11]]);
  }));

  return function (_x) {
    return _ref2.apply(this, arguments);
  };
}());

module.exports = connector;