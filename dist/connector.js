"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports["default"] = void 0;

var _connectionStringParser = require("connection-string-parser");

var _mongodb = require("mongodb");

var _errors = require("./errors");

function asyncGeneratorStep(gen, resolve, reject, _next, _throw, key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { Promise.resolve(value).then(_next, _throw); } }

function _asyncToGenerator(fn) { return function () { var self = this, args = arguments; return new Promise(function (resolve, reject) { var gen = fn.apply(self, args); function _next(value) { asyncGeneratorStep(gen, resolve, reject, _next, _throw, "next", value); } function _throw(err) { asyncGeneratorStep(gen, resolve, reject, _next, _throw, "throw", err); } _next(undefined); }); }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

var connectionStringParser = new _connectionStringParser.ConnectionStringParser({
  scheme: "mongodb",
  hosts: []
});

var Connector =
/*#__PURE__*/
function () {
  function Connector() {
    _classCallCheck(this, Connector);
  }

  _createClass(Connector, [{
    key: "replSet",
    value: function replSet(configs) {
      if (Array.isArray(configs) && configs.length > 0) {
        var servers = configs.map(function (config) {
          return new _mongodb.Server(config.host, parseInt(mongoConfig.port, 10));
        });
        var replSet = new _mongodb.ReplSet(servers);
        return replSet;
      }

      var connectionObject = connectionStringParser.parse(Connector.credentials.connectionString);
      return new _mongodb.ReplSet(connectionObject.hosts);
    }
    /**
     * Disconnects current MongoDB connection or throws an error
     * if no connection exists
     *
     * @static
     * @memberof Connector
     *
     * @returns {void}
     */

  }], [{
    key: "connect",

    /**
     * Connected MongoDB client instance
     *
     * @static
     * @type {Object|null}
     */

    /**
     * Current connection's status
     *
     * @static
     * @type {boolean}
     */

    /**
     * Current connection's credentials
     *
     * @static
     * @type {Object}
     * @param {string} credentials
     */

    /**
     * Creates a MongoDB connection
     *
     * @static
     *
     * @param {Object} credentials credentials object
     * @param {string} credentials.connectionString MongoDB connection url
     *
     * @returns {Promise<Object>} dbClient
     */
    value: function () {
      var _connect = _asyncToGenerator(
      /*#__PURE__*/
      regeneratorRuntime.mark(function _callee(_ref) {
        var connectionString, dbClient;
        return regeneratorRuntime.wrap(function _callee$(_context) {
          while (1) {
            switch (_context.prev = _context.next) {
              case 0:
                connectionString = _ref.connectionString;
                _context.next = 3;
                return _mongodb.MongoClient.connect(connectionString, {
                  poolSize: 200,
                  useNewUrlParser: true
                });

              case 3:
                dbClient = _context.sent;
                Connector.client = dbClient;
                Connector.connected = true;
                Connector.connectionString = connectionString;
                return _context.abrupt("return", dbClient);

              case 8:
              case "end":
                return _context.stop();
            }
          }
        }, _callee);
      }));

      function connect(_x) {
        return _connect.apply(this, arguments);
      }

      return connect;
    }()
  }, {
    key: "disconnect",
    value: function disconnect() {
      if (Connector.connected) {
        Connector.client.close();
        Connector.connected = false;
        Connector.credentials = {
          connectionString: ''
        };
      } else {
        throw new _errors.DatabaseConnectionError();
      }
    }
  }]);

  return Connector;
}();

_defineProperty(Connector, "client", null);

_defineProperty(Connector, "connected", false);

_defineProperty(Connector, "credentials", {
  connectionString: ''
});

var _default = Connector;
exports["default"] = _default;