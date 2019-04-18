"use strict";

function asyncGeneratorStep(gen, resolve, reject, _next, _throw, key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { Promise.resolve(value).then(_next, _throw); } }

function _asyncToGenerator(fn) { return function () { var self = this, args = arguments; return new Promise(function (resolve, reject) { var gen = fn.apply(self, args); function _next(value) { asyncGeneratorStep(gen, resolve, reject, _next, _throw, "next", value); } function _throw(err) { asyncGeneratorStep(gen, resolve, reject, _next, _throw, "throw", err); } _next(undefined); }); }; }

function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance"); }

function _iterableToArrayLimit(arr, i) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

/* eslint-disable no-redeclare, no-undef */

/*
#     CloudBoost - Core Engine that powers Bakend as a Service
#     (c) 2014 HackerBay, Inc.
#     CloudBoost may be freely distributed under the Apache 2 License
*/

/* eslint no-use-before-define: 0, no-param-reassign: 0 */
var q = require('q');

var _ = require('underscore');

var Grid = require('gridfs-stream');

var winston = require('winston');

var mongodb = require('mongodb');

var connector = require('./connector');

var util = require('./util');

var document = {
  get: function get(appId, collectionName, documentId, accessList, isMasterKey) {
    // returns the document that matches the _id with the documentId
    var deferred = q.defer();

    try {
      document.findOne(appId, collectionName, {
        _id: documentId
      }, null, null, null, accessList, isMasterKey).then(function (doc) {
        deferred.resolve(doc);
      }, function (error) {
        winston.log('error', error);
        deferred.reject(error);
      });
    } catch (err) {
      winston.log('error', {
        error: String(err),
        stack: new Error().stack
      });
      deferred.reject(err);
    }

    return deferred.promise;
  },
  _include: function _include(appId, include, docs) {
    // This function is for joins. :)
    var join = [];
    var deferred = q.defer();

    try {
      // include and merge all the documents.
      var promises = [];
      include.sort();

      var _loop = function _loop(_i) {
        var _include$_i$split = include[_i].split('.'),
            _include$_i$split2 = _slicedToArray(_include$_i$split, 1),
            columnName = _include$_i$split2[0];

        join.push(columnName);

        for (var k = 1; k < include.length; k++) {
          if (columnName === include[k].split('.')[0]) {
            _i += 1;
          } else {
            break;
          }
        } // include this column and merge.


        var idList = [];
        var collectionName = null;

        _.each(docs, function (doc) {
          if (doc[columnName] !== null) {
            // checks if the doc[columnName] is an list of relations or a relation
            if (Object.getPrototypeOf(doc[columnName]) === Object.prototype) {
              if (doc[columnName] && doc[columnName]._id) {
                if (doc[columnName]._type === 'file') {
                  collectionName = '_File';
                } else {
                  collectionName = doc[columnName]._tableName;
                }

                idList.push(doc[columnName]._id);
              }
            } else {
              for (var j = 0; j < doc[columnName].length; j++) {
                if (doc[columnName][j] && doc[columnName][j]._id) {
                  if (doc[columnName][j]._type === 'file') {
                    collectionName = '_File';
                  } else {
                    collectionName = doc[columnName][j]._tableName;
                  }

                  idList.push(doc[columnName][j]._id);
                }
              }
            }
          }
        }, null); // if(idList.length >0 && collectionName) {


        var qry = {};
        qry._id = {};
        qry._id.$in = idList;
        promises.push(document.fetch_data(appId, collectionName, qry)); // }

        i = _i;
      };

      for (var i = 0; i < include.length; i++) {
        _loop(i);
      }

      q.all(promises).then(function (arrayOfDocs) {
        var pr = [];
        var rInclude = [];

        for (var _i2 = 0; _i2 < join.length; _i2++) {
          for (var k = 0; k < include.length; k++) {
            if (join[_i2] === include[k].split('.')[0]) rInclude.push(include[k]);
          }

          for (var _k = 0; _k < rInclude.length; _k++) {
            rInclude[_k] = rInclude[_k].split('.').splice(1, 1).join('.');
          }

          for (var _k2 = 0; _k2 < rInclude.length; _k2++) {
            if (rInclude[_k2] === join[_i2] || rInclude[_k2] === '') {
              rInclude.splice(_k2, 1);
              _k2 -= 1;
            }
          }

          if (rInclude.length > 0) {
            pr.push(document._include(appId, rInclude, arrayOfDocs[_i2]));
          } else {
            var newPromise = q.defer();
            newPromise.resolve(arrayOfDocs[_i2]);
            pr.push(newPromise.promise);
          }
        }

        q.all(pr).then(function (_arrayOfDocs) {
          for (var _i3 = 0; _i3 < docs.length; _i3++) {
            for (var j = 0; j < join.length; j++) {
              // if the doc contains an relation with a columnName.
              var relationalDoc = docs[_i3][join[j]];

              if (relationalDoc) {
                var rel = null;

                if (relationalDoc.constructor === Array) {
                  for (var m = 0; m < relationalDoc.length; m++) {
                    for (var _k3 = 0; _k3 < _arrayOfDocs[j].length; _k3++) {
                      if (_arrayOfDocs[j][_k3]._id.toString() === relationalDoc[m]._id.toString()) {
                        rel = _arrayOfDocs[j][_k3];
                        break;
                      }
                    }

                    if (rel) {
                      docs[_i3][include[j]][m] = rel;
                    }
                  }
                } else {
                  for (var _k4 = 0; _k4 < _arrayOfDocs[j].length; _k4++) {
                    if (_arrayOfDocs[j][_k4]._id.toString() === relationalDoc._id.toString()) {
                      rel = _arrayOfDocs[j][_k4];
                      break;
                    }
                  }

                  if (rel) {
                    docs[_i3][join[j]] = rel;
                  }
                }
              }
            }
          }

          docs = _deserialize(docs);
          deferred.resolve(docs);
        }, function (error) {
          winston.log('error', error);
          deferred.reject(error);
        });
      }, function (error) {
        winston.log('error', error);
        deferred.reject();
      });
    } catch (err) {
      winston.log('error', {
        error: String(err),
        stack: new Error().stack
      });
      deferred.reject(err);
    }

    return deferred.promise;
  },
  fetch_data: function fetch_data(appId, collectionName, qry) {
    var includeDeferred = q.defer();

    try {
      if (connector.connected === false) {
        includeDeferred.reject('Database Not Connected');
        return includeDeferred.promise;
      }

      if (!collectionName || !qry._id.$in) {
        includeDeferred.resolve([]);
        return includeDeferred.promise;
      }

      connector.client.db(appId).collection(util.collection.getId(appId, collectionName)).find(qry).toArray(function (err, includeDocs) {
        if (err) {
          winston.log('error', err);
          includeDeferred.reject(err);
        } else {
          includeDeferred.resolve(includeDocs);
        }
      });
    } catch (err) {
      winston.log('error', {
        error: String(err),
        stack: new Error().stack
      });
      includeDeferred.reject(err);
    }

    return includeDeferred.promise;
  },
  find: function find(appId, collectionName, query, select, sort, limit, skip, accessList, isMasterKey) {
    var deferred = q.defer();

    try {
      if (connector.connected === false) {
        deferred.reject('Database Not Connected');
        return deferred.promise;
      }

      var collection = connector.client.db(appId).collection(util.collection.getId(appId, collectionName));
      var include = [];
      /* query for expires */

      if (!query.$or) {
        query.$or = [{
          expires: null
        }, {
          expires: {
            $gte: new Date()
          }
        }];
      } else {
        oldQuery = query.$or;

        if (oldQuery[0].$include) {
          if (oldQuery[0].$include.length > 0) {
            include = include.concat(oldQuery[0].$include);
          }

          delete oldQuery[0].$include;
          delete oldQuery[0].$includeList;
        }

        if (oldQuery[1]) {
          if (oldQuery[1].$include) {
            if (oldQuery[1].$include.length > 0) {
              include = include.concat(oldQuery[1].$include);
            }

            delete oldQuery[1].$include;
            delete oldQuery[1].$includeList;
          }
        }

        query.$and = [{
          $or: oldQuery
        }, {
          $or: [{
            expires: null
          }, {
            expires: {
              $gte: new Date().getTime()
            }
          }]
        }];
        delete query.$or;
      }

      if (!select || Object.keys(select).length === 0) {
        select = {};
      } else {
        // defult columns which should be selected.
        select.ACL = 1;
        select.createdAt = 1;
        select.updatedAt = 1;
        select._id = 1;
        select._tableName = 1;
        select._type = 1;
        select.expires = 1;
      }

      if (!sort) {
        sort = {};
      } // default sort added

      /*
        without sort if limit and skip are used,
        the records are returned out of order.
        To solve this default sort in ascending order of 'createdAt' is added
      */


      if (!sort.createdAt) sort.createdAt = 1;
      if (!limit || limit === -1) limit = 20;

      if (!isMasterKey) {
        // if its not master key then apply ACL.
        if (accessList.userId) {
          var aclQuery = [{
            $or: [{
              'ACL.read.allow.user': 'all'
            }, {
              'ACL.read.allow.user': accessList.userId
            }, {
              'ACL.read.allow.role': {
                $in: accessList.roles
              }
            }]
          }, {
            $and: [{
              'ACL.read.deny.user': {
                $ne: accessList.userId
              }
            }, {
              'ACL.read.deny.role': {
                $nin: accessList.roles
              }
            }]
          }];
          if (query.$and) query.$and.push({
            $and: aclQuery
          });else query.$and = aclQuery;
        } else {
          query['ACL.read.allow.user'] = 'all';
        }
      } // check for include.


      if (query.$include) {
        if (query.$include.length > 0) {
          include = include.concat(query.$include);
        }
      } // delete $include and $includeList recursively


      query = _sanitizeQuery(query);
      var findQuery = collection.find(query).project(select);

      if (Object.keys(sort).length > 0) {
        findQuery = findQuery.sort(sort);
      }

      if (skip) {
        if (Object.keys(sort).length === 0) {
          // default sort it in desc order on createdAt
          findQuery = findQuery.sort({
            createdAt: -1
          });
        }

        findQuery = findQuery.skip(skip);
      }

      findQuery = findQuery.limit(limit);
      findQuery.toArray().then(function (docs) {
        if (!include || include.length === 0) {
          docs = _deserialize(docs);
          deferred.resolve(docs);
        } else {
          obj.document._include(appId, include, docs).then(deferred.resolve, deferred.reject);
        }
      })["catch"](deferred.reject);
    } catch (err) {
      winston.log('error', {
        error: String(err),
        stack: new Error().stack
      });
      deferred.reject(err);
    }

    return deferred.promise;
  },
  findOne: function findOne(appId, collectionName, query, select, sort, skip, accessList, isMasterKey) {
    var mainPromise = q.defer();

    try {
      if (connector.connected === false) {
        mainPromise.reject('Database Not Connected');
        return mainPromise.promise;
      }

      obj.document.find(appId, collectionName, query, select, sort, 1, skip, accessList, isMasterKey).then(function (list) {
        if (Object.prototype.toString.call(list) === '[object Array]') {
          if (list.length === 0) {
            mainPromise.resolve(null);
          } else {
            mainPromise.resolve(list[0]);
          }
        }
      }, function (error) {
        winston.log('error', error);
        mainPromise.reject(null);
      });
    } catch (err) {
      winston.log('error', {
        error: String(err),
        stack: new Error().stack
      });
      mainPromise.reject(err);
    }

    return mainPromise.promise;
  },
  save: function save(appId, documentArray) {
    var deferred = q.defer();

    try {
      if (connector.connected === false) {
        deferred.reject('Database Not Connected');
        return deferred.promise;
      }

      var promises = [];

      for (var _i4 = 0; _i4 < documentArray.length; _i4++) {
        promises.push(_save(appId, documentArray[_i4].document._tableName, documentArray[_i4].document));
      }

      q.allSettled(promises).then(function (docs) {
        deferred.resolve(docs);
      }, function (err) {
        winston.log('error', err);
        deferred.reject(err);
      });
    } catch (err) {
      winston.log('error', {
        error: String(err),
        stack: new Error().stack
      });
      deferred.reject(err);
    }

    return deferred.promise;
  },
  _update: function _update(appId, collectionName, document) {
    var deferred = q.defer();

    try {
      if (connector.connected === false) {
        deferred.reject('Database Not Connected');
        return deferred.promise;
      }

      var collection = connector.client.db(appId).collection(util.collection.getId(appId, collectionName));
      var documentId = document._id;
      var query = {};
      query._id = documentId;
      collection.update({
        _id: documentId
      }, document, {
        upsert: true
      }, function (err, list) {
        if (err) {
          winston.log('error', err);
          deferred.reject(err);
        } else if (list) {
          deferred.resolve(document);
        }
      });
    } catch (err) {
      winston.log('error', {
        error: String(err),
        stack: new Error().stack
      });
      deferred.reject(err);
    }

    return deferred.promise;
  },
  count: function count(appId, collectionName, query, limit, skip) {
    var deferred = q.defer();

    try {
      if (connector.connected === false) {
        deferred.reject('Database Not Connected');
        return deferred.promise;
      }

      if (skip) {
        skip = parseInt(skip, 10);
      }

      var collection = connector.client.db(appId).collection(util.collection.getId(appId, collectionName)); // delete $include and $includeList recursively

      query = _sanitizeQuery(query);
      var findQuery = collection.find(query);

      if (skip) {
        findQuery = findQuery.skip(skip);
      }

      findQuery.count(query, function (err, count) {
        if (err) {
          winston.log('error', err);
          deferred.reject(err);
        } else {
          deferred.resolve(count);
        }
      });
    } catch (err) {
      winston.log('error', {
        error: String(err),
        stack: new Error().stack
      });
      deferred.reject(err);
    }

    return deferred.promise;
  },
  distinct: function distinct(appId, collectionName, onKey, query, select, sort, limit, skip) {
    var deferred = q.defer();

    try {
      if (connector.connected === false) {
        deferred.reject('Database Not Connected');
        return deferred.promise;
      }

      var collection = connector.client.db(appId).collection(util.collection.getId(appId, collectionName));
      var include = [];

      if (query.$include) {
        if (query.$include.length > 0) {
          include = include.concat(query.$include);
        }
      } // delete $include and $includeList recursively


      query = _sanitizeQuery(query);
      var keys = {};
      var indexForDot = onKey.indexOf('.'); // if DOT in onKey
      //  keys = { beforeDot: { afterDot : "$beforeDot.afterDot"} }
      // else
      //  keys = { onKey : "$"+onKey }

      if (indexForDot !== -1) {
        // not using computed properties as it may not be available in server's nodejs version
        keys[onKey.slice(0, indexForDot)] = {};
        keys[onKey.slice(0, indexForDot)][onKey.slice(indexForDot + 1)] = "$".concat(onKey);
      } else keys[onKey] = "$".concat(onKey);

      if (!sort || Object.keys(sort).length === 0) sort = {
        createdAt: 1
      };

      if (!query || Object.keys(query).length === 0) {
        query = {
          _id: {
            $exists: true
          }
        };
      }

      var pipeline = [];
      pipeline.push({
        $match: query
      });
      pipeline.push({
        $sort: sort
      }); // push the distinct aggregation.

      pipeline.push({
        $group: {
          _id: keys,
          document: {
            $first: '$$ROOT'
          }
        }
      });

      if (skip && skip !== 0) {
        pipeline.push({
          $skip: skip
        });
      }

      if (limit && limit !== 0) {
        pipeline.push({
          $limit: limit
        });
      }

      if (select && Object.keys(select).length > 0) {
        pipeline.push({
          $project: {
            document: select
          }
        });
      }

      collection.aggregate(pipeline, function (err, cursor) {
        if (err) {
          deferred.reject(err);
        } else {
          var docs = [];
          cursor.toArray().then(function (res) {
            // filter out
            for (var _i5 = 0; _i5 < res.length; _i5++) {
              docs.push(res[_i5].document);
            } // include.


            obj.document._include(appId, include, docs).then(function (docList) {
              docs = _deserialize(docList);
              deferred.resolve(docs);
            }, function (error) {
              winston.log('error', {
                error: String(error),
                stack: new Error().stack
              });
              deferred.reject(error);
            });
          })["catch"](function (error) {
            winston.log('error', {
              error: String(error),
              stack: new Error().stack
            });
            deferred.reject(error);
          });
        }
      });
    } catch (err) {
      winston.log('error', {
        error: String(err),
        stack: new Error().stack
      });
      deferred.reject(err);
    }

    return deferred.promise;
  },
  aggregate: function aggregate(appId, collectionName, pipeline, limit, skip, accessList, isMasterKey) {
    var deferred = q.defer();

    try {
      if (connector.connected === false) {
        deferred.reject('Database Not Connected');
        return deferred.promise;
      }

      var collection = connector.client.db(appId).collection(util.collection.getId(appId, collectionName));
      var query = {};

      if (pipeline.length > 0 && pipeline[0] && pipeline[0].$match) {
        query = pipeline[0].$match;
        pipeline.shift(); // remove first element.
      }

      if (!isMasterKey) {
        // if its not master key then apply ACL.
        if (accessList.userId) {
          var aclQuery = [{
            $or: [{
              'ACL.read.allow.user': 'all'
            }, {
              'ACL.read.allow.user': accessList.userId
            }, {
              'ACL.read.allow.role': {
                $in: accessList.roles
              }
            }]
          }, {
            $and: [{
              'ACL.read.deny.user': {
                $ne: accessList.userId
              }
            }, {
              'ACL.read.deny.role': {
                $nin: accessList.roles
              }
            }]
          }];
          if (query.$and) query.$and.push({
            $and: aclQuery
          });else query.$and = aclQuery;
        } else {
          query['ACL.read.allow.user'] = 'all';
        }
      }

      if (!query.$or) {
        query.$or = [{
          expires: null
        }, {
          expires: {
            $gte: new Date()
          }
        }];
      }

      pipeline.unshift({
        $match: query
      }); // add item to the begining of the pipeline.

      if (skip && skip !== 0) {
        pipeline.push({
          $skip: skip
        });
      }

      if (limit && limit !== 0) {
        pipeline.push({
          $limit: limit
        });
      }

      collection.aggregate(pipeline, function (err, res) {
        if (err) {
          deferred.reject(err);
        } else {
          deferred.resolve(res);
        }
      });
    } catch (err) {
      winston.log('error', {
        error: String(err),
        stack: new Error().stack
      });
      deferred.reject(err);
    }

    return deferred.promise;
  },
  _insert: function _insert(appId, collectionName, document) {
    var deferred = q.defer();

    try {
      if (connector.connected === false) {
        deferred.reject('Database Not Connected');
        return deferred.promise;
      }

      var collection = connector.client.db(appId).collection(util.collection.getId(appId, collectionName));
      collection.save(document, function (err, doc) {
        if (err) {
          winston.log('error', err);
          deferred.reject(err);
        } else {
          // elastic search code.
          document = doc;
          deferred.resolve(document);
        }
      });
    } catch (err) {
      winston.log('error', {
        error: String(err),
        stack: new Error().stack
      });
      deferred.reject(err);
    }

    return deferred.promise;
  },
  "delete": function _delete(appId, collectionName, document) {
    var documentId = document._id;
    var deferred = q.defer();

    try {
      if (connector.connected === false) {
        deferred.reject('Database Not Connected');
        return deferred.promise;
      }

      if (!document._id) {
        deferred.reject('You cant delete an unsaved object');
      } else {
        var collection = connector.client.db(appId).collection(util.collection.getId(appId, collectionName));
        var query = {
          _id: documentId
        };
        collection.remove(query, {
          w: 1 // returns the number of documents removed

        }, function (err, doc) {
          if (err || doc.result.n === 0) {
            if (doc.result.n === 0) {
              err = {
                code: 401,
                message: 'You do not have permission to delete'
              };
              winston.log('error', err);
              deferred.reject(err);
            }
          }

          if (err) {
            winston.log('error', err);
            deferred.reject(err);
          } else if (doc.result.n !== 0) {
            deferred.resolve(doc.result);
          } else {
            deferred.reject({
              code: 500,
              message: 'Server Error'
            });
          }
        });
      }
    } catch (err) {
      winston.log('error', {
        error: String(err),
        stack: new Error().stack
      });
      deferred.reject(err);
    }

    return deferred.promise;
  },
  deleteByQuery: function deleteByQuery(appId, collectionName, query) {
    var deferred = q.defer();

    try {
      if (connector.connected === false) {
        deferred.reject('Database Not Connected');
        return deferred.promise;
      }

      var collection = connector.client.db(appId).collection(util.collection.getId(appId, collectionName));
      collection.remove(query, {
        w: 1 // returns the number of documents removed

      }, function (err, doc) {
        if (err) {
          winston.log('error', err);
          deferred.reject(err);
        }

        deferred.resolve(doc.result);
      });
    } catch (err) {
      winston.log('error', {
        error: String(err),
        stack: new Error().stack
      });
      deferred.reject(err);
    }

    return deferred.promise;
  },

  /** ********************GRIDFS FILES************************************************************** */

  /* Desc   : Get file from gridfs
      Params : appId,filename
      Returns: Promise
               Resolve->file
               Reject->Error on findOne() or file not found(null)
    */
  getFile: function getFile(appId, filename) {
    var deferred = q.defer();

    try {
      connector.client.db(appId).collection('fs.files').findOne({
        filename: filename
      }, function (err, file) {
        if (err) {
          deferred.reject(err);
        }

        if (!file) {
          deferred.resolve(null);
        }

        deferred.resolve(file);
      });
    } catch (err) {
      winston.log('error', {
        error: String(err),
        stack: new Error().stack
      });
      deferred.reject(err);
    }

    return deferred.promise;
  },

  /* Desc   : Get fileStream from gridfs
      Params : appId,fileId
      Returns: fileStream
    */
  getFileStreamById: function getFileStreamById(appId, fileId) {
    try {
      var gfs = Grid(connector.client.db(appId), mongodb);
      var readstream = gfs.createReadStream({
        _id: fileId
      });
      return readstream;
    } catch (err) {
      winston.log('error', {
        error: String(err),
        stack: new Error().stack
      });
      return null;
    }
  },

  /* Desc   : Delete file from gridfs
      Params : appId,filename
      Returns: Promise
               Resolve->true
               Reject->Error on exist() or remove() or file does not exists
    */
  deleteFileFromGridFs: function () {
    var _deleteFileFromGridFs = _asyncToGenerator(
    /*#__PURE__*/
    regeneratorRuntime.mark(function _callee(appId, filename) {
      var deferred, found, id;
      return regeneratorRuntime.wrap(function _callee$(_context) {
        while (1) {
          switch (_context.prev = _context.next) {
            case 0:
              deferred = q.defer();
              _context.prev = 1;
              _context.next = 4;
              return connector.client.db(appId).collection('fs.files').findOne({
                filename: filename
              });

            case 4:
              found = _context.sent;

              if (found) {
                id = found._id;
                connector.client.db(appId).collection('fs').deleteMany({
                  _id: id
                }, function (err) {
                  if (err) {
                    // Unable to delete
                    deferred.reject(err);
                  } else {
                    // Deleted
                    deferred.resolve(true);
                  }

                  return deferred.resolve('Success');
                });
              } else {
                deferred.reject('File does not exist');
              }

              _context.next = 12;
              break;

            case 8:
              _context.prev = 8;
              _context.t0 = _context["catch"](1);
              winston.log('error', {
                error: String(_context.t0),
                stack: new Error().stack
              });
              deferred.reject(_context.t0);

            case 12:
              return _context.abrupt("return", deferred.promise);

            case 13:
            case "end":
              return _context.stop();
          }
        }
      }, _callee, null, [[1, 8]]);
    }));

    function deleteFileFromGridFs(_x, _x2) {
      return _deleteFileFromGridFs.apply(this, arguments);
    }

    return deleteFileFromGridFs;
  }(),

  /* Desc   : Save filestream to gridfs
      Params : appId,fileStream,fileName,contentType
      Returns: Promise
               Resolve->fileObject
               Reject->Error on writing file
    */
  saveFileStream: function saveFileStream(appId, fileStream, fileName, contentType) {
    var deferred = q.defer();

    try {
      var bucket = new mongodb.GridFSBucket(connector.client.db(appId));
      var writeStream = bucket.openUploadStream(fileName, {
        contentType: contentType,
        w: 1
      });
      fileStream.pipe(writeStream).on('error', function (err) {
        deferred.reject(err);
        writeStream.destroy();
      }).on('finish', function (file) {
        deferred.resolve(file);
      });
    } catch (err) {
      winston.log('error', {
        error: String(err),
        stack: new Error().stack
      });
      deferred.reject(err);
    }

    return deferred.promise;
  }
};
module.exports = document;
/* Private functions */

function _sanitizeQuery(query) {
  if (query && query.$includeList) {
    delete query.$includeList;
  }

  if (query && query.$include) {
    delete query.$include;
  }

  if (query && query.$or && query.$or.length > 0) {
    for (var _i6 = 0; _i6 < query.$or.length; ++_i6) {
      query.$or[_i6] = _sanitizeQuery(query.$or[_i6]);
    }
  }

  if (query && query.$and && query.$and.length > 0) {
    for (var _i7 = 0; _i7 < query.$and.length; ++_i7) {
      query.$and[_i7] = _sanitizeQuery(query.$and[_i7]);
    }
  }

  return query;
}

function _save(appId, collectionName, document) {
  var deferredMain = q.defer();

  try {
    if (document._isModified) {
      delete document._isModified;
    }

    if (document._modifiedColumns) {
      delete document._modifiedColumns;
    }

    document = _serialize(document); // column key array to track sub documents.

    obj.document._update(appId, collectionName, document).then(function (doc) {
      doc = _deserialize(doc);
      deferredMain.resolve(doc);
    }, function (err) {
      winston.log('error', err);
      deferredMain.reject(err);
    });
  } catch (err) {
    winston.log('error', {
      error: String(err),
      stack: new Error().stack
    });
    deferred.reject(err);
  }

  return deferredMain.promise;
}

function _serialize(document) {
  try {
    Object.keys(document).forEach(function (key) {
      if (document[key]) {
        if (document[key].constructor === Object && document[key]._type) {
          if (document[key]._type === 'point') {
            var _obj = {};
            _obj.type = 'Point';
            _obj.coordinates = document[key].coordinates;
            document[key] = _obj;
          }
        }

        if (key === 'createdAt' || key === 'updatedAt' || key === 'expires') {
          if (typeof document[key] === 'string') {
            document[key] = new Date(document[key]);
          }
        }
      }
    });
    return document;
  } catch (err) {
    winston.log('error', {
      error: String(err),
      stack: new Error().stack
    });
    throw err;
  }
}

function _deserialize(docs) {
  try {
    if (docs.length > 0) {
      for (var _i8 = 0; _i8 < docs.length; _i8++) {
        var _document = docs[_i8];
        var docKeys = Object.keys(_document);

        for (var j = 0; j < docKeys.length; j++) {
          var key = docKeys[j];

          if (_document[key]) {
            if (_document[key].constructor === Object && _document[key].type) {
              if (_document[key].type === 'Point') {
                var _obj = {};
                _obj._type = 'point';
                _obj.coordinates = _document[key].coordinates;
                _obj.latitude = _obj.coordinates[1]; // eslint-disable-line

                _obj.longitude = _obj.coordinates[0]; // eslint-disable-line

                _document[key] = _obj;
              }
            } else if (_document[key].constructor === Array && _document[key][0] && _document[key][0].type && _document[key][0].type === 'Point') {
              var arr = [];

              for (var k = 0; k < _document[key].length; k++) {
                var _obk = {};
                _obk._type = 'point';
                _obk.coordinates = _document[key][k].coordinates;
                _obk.latitude = _obk.coordinates[1]; // eslint-disable-line

                _obk.longitude = _obk.coordinates[0]; // eslint-disable-line

                arr.push(_obk);
              }

              _document[key] = arr;
            }
          }
        }

        docs[_i8] = _document;
      }
    } else {
      var _document2 = docs;

      var _docKeys = Object.keys(_document2);

      for (var _k5 = 0; _k5 < _docKeys.length; _k5++) {
        var _key = _docKeys[_k5];

        if (_document2[_key]) {
          if (_document2[_key].constructor === Object && _document2[_key].type) {
            if (_document2[_key].type === 'Point') {
              var _obj2 = {};
              _obj2._type = 'point';
              _obj2.coordinates = _document2[_key].coordinates;
              _obj2.latitude = _obj2.coordinates[1]; // eslint-disable-line

              _obj2.longitude = _obj2.coordinates[0]; // eslint-disable-line

              _document2[_key] = _obj2;
            }
          } else if (_document2[_key].constructor === Array && _document2[_key][0] && _document2[_key][0].type && _document2[_key][0].type === 'Point') {
            var _arr2 = [];

            for (var _j = 0; _j < _document2[_key].length; _j++) {
              var _obj3 = {};
              _obj3._type = 'point';
              _obj3.coordinates = _document2[_key][_j].coordinates;
              _obj3.latitude = _obj3.coordinates[1]; // eslint-disable-line

              _obj3.longitude = _obj3.coordinates[0]; // eslint-disable-line

              _arr2.push(_obj3);
            }

            _document2[_key] = _arr2;
          }
        }
      }

      docs = _document2;
    }

    return docs;
  } catch (err) {
    winston.log('error', {
      error: String(err),
      stack: new Error().stack
    });
    throw err;
  }
}