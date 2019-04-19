import { ConnectionStringParser } from 'connection-string-parser';
import mongodb from 'mongodb'
import mongoUtil from './util';
import errors from './errors';

const { ReplSet, Server, MongoClient } = mongodb;
const connectionStringParser = new ConnectionStringParser({
  scheme: "mongodb",
  hosts: []
});

class MongoAdapter {
  /**
   * Connected MongoDB client instance
   *
   * @type {Object|null}
   */
  client = null;

  /**
   * Current connection's status
   *
   * @type {boolean}
   */
  connected = false;

  /**
   * Current connection's credentials
   *
   * @type {Object}
   * @param {string} credentials
   */
  credentials = {
    connectionString: ''
  };

  constructor (credentials) {
    this.credentials = credentials
  }

  /**
   * Creates a MongoDB connection
   *
   * @param {Object} credentials credentials object
   * @param {string} credentials.connectionString MongoDB connection url
   *
   * @returns {Promise<Object>} dbClient
   */
  connect = async ({ connectionString }) => {
    const dbClient = await MongoClient.connect(connectionString, {
      poolSize: 200,
      useNewUrlParser: true,
    });

    this.client = dbClient;
    this.connected = true;
    this.connectionString = connectionString;

    return dbClient;
  }

  replSet (configs) {
    if (Array.isArray(configs) && configs.length > 0) {
      const servers = configs.map(config => {
        return new Server(config.host, parseInt(mongoConfig.port, 10))
      });

      const replSet = new ReplSet(servers);

      return replSet;
    }

    const connectionObject = connectionStringParser.parse(this.credentials.connectionString);

    return new ReplSet(connectionObject.hosts);
  }

  /**
   * Disconnects current MongoDB connection or throws an error
   * if no connection exists
   *
   * @memberof Connector
   *
   * @returns {void}
   */
  disconnect () {
    if (this.connected) {
      this.client.close();
      this.connected = false;
      this.credentials = {
        connectionString: ''
      };
    } else {
      throw new errors.DatabaseConnectionError();
    }
  }

  /**
   * Returns the document that matches the _id with the documentId
   *
   * @param {Object} details
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {string} details.documentId
   * @param {Object} details.accessList
   * @param {boolean} details.isMasterKey
   *
   * @returns {Promise<object>} document
   */
  findById = async ({
    appId,
    collectionName,
    documentId,
    accessList,
    isMasterKey
  }) => {
    return this.findOne({
      appId,
      collectionName,
      query: {
        _id: documentId
      },
      accessList,
      isMasterKey
    });
  }

  /**
   *
   * @param {string} appId
   * @param {Array} include
   * @param {Array} docs
   *
   * @returns {Promise<Array>} documents
   */
  _include = async (appId, include, docs) => {
    const currentDocs = [ ...docs ];

    // This function is for joins. :)
    const join = [];

    // include and merge all the documents.
    const promises = [];
    include.sort();

    for (let i = 0; i < include.length; i++) {
      const [columnName] = include[i].split('.');
      join.push(columnName);
      for (let k = 1; k < include.length; k++) {
        if (columnName === include[k].split('.')[0]) {
          i += 1;
        } else {
          break;
        }
      }
      // include this column and merge.
      const idList = [];
      let collectionName = null;

      currentDocs.forEach((doc) => {
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
            for (let j = 0; j < doc[columnName].length; j++) {
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
      });

      const query = {
        _id: {
          $in: idList
        }
      };

      promises.push(this.fetch_data(appId, collectionName, query));
    }

    return Promise.all(promises).then((arrayOfDocs) => {
      const pr = [];
      const rInclude = [];

      for (let i = 0; i < join.length; i++) {
        for (let k = 0; k < include.length; k++) {
          if (join[i] === include[k].split('.')[0]) rInclude.push(include[k]);
        }
        for (let k = 0; k < rInclude.length; k++) {
          rInclude[k] = rInclude[k].split('.').splice(1, 1).join('.');
        }
        for (let k = 0; k < rInclude.length; k++) {
          if (rInclude[k] === join[i] || rInclude[k] === '') {
            rInclude.splice(k, 1);
            k -= 1;
          }
        }
        if (rInclude.length > 0) {
          pr.push(this._include(appId, rInclude, arrayOfDocs[i]));
        } else {
          pr.push(Promise.resolve(arrayOfDocs[i]));
        }
      }

      return Promise.all(pr).then((_arrayOfDocs) => {
        for (let i = 0; i < currentDocs.length; i++) {
          for (let j = 0; j < join.length; j++) {
            // if the doc contains an relation with a columnName.
            const relationalDoc = currentDocs[i][join[j]];

            if (relationalDoc) {
              let rel = null;
              if (Array.isArray(relationalDoc)) {
                for (let m = 0; m < relationalDoc.length; m++) {
                  for (let k = 0; k < _arrayOfDocs[j].length; k++) {
                    if (_arrayOfDocs[j][k]._id.toString() === relationalDoc[m]._id.toString()) {
                      rel = _arrayOfDocs[j][k];
                      break;
                    }
                  }
                  if (rel) {
                    currentDocs[i][include[j]][m] = rel;
                  }
                }
              } else {
                for (let k = 0; k < _arrayOfDocs[j].length; k++) {
                  if (_arrayOfDocs[j][k]._id.toString() === relationalDoc._id.toString()) {
                    rel = _arrayOfDocs[j][k];
                    break;
                  }
                }

                if (rel) {
                  currentDocs[i][join[j]] = rel;
                }
              }
            }
          }
        }

        const deserializedDocs = _deserialize(currentDocs);

        return deserializedDocs;
      });
    });
  }

  /**
   *
   * @param {string} appId
   * @param {string} collectionName
   * @param {Object} qry
   *
   * @returns {Promise<Array>} documents
   */
  fetch_data = async (appId, collectionName, qry) => {
    if (this.connected === false) {
      throw new errors.DatabaseConnectionError();
    }

    if (!collectionName || !qry._id.$in) {
      return [];
    }

    return new Promise((resolve, reject) => {
      this.client.db(appId)
        .collection(util.collection.getId(appId, collectionName))
        .find(qry)
        .toArray((err, includeDocs) => {
          if (err) {
            return reject(err);
          }

          return resolve(includeDocs);
        });
    })
  }

  /**
   *
   * @param {Object} details
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Object} details.query
   * @param {Object} details.select
   * @param {Object} details.sort
   * @param {number} details.limit
   * @param {Object} details.skip
   * @param {Object} details.accessList
   * @param {boolean} details.isMasterKey
   */
  find = async ({
    appId,
    collectionName,
    query,
    select,
    sort,
    limit,
    skip,
    accessList,
    isMasterKey
  }) => {
    if (this.connected === false) {
      throw new errors.DatabaseConnectionError();
    }

    const collection = this.client
      .db(appId)
      .collection(util.collection.getId(appId, collectionName));

    let include = [];
    /* query for expires */

    if (!query.$or) {
      query.$or = [
        {
          expires: null,
        }, {
          expires: {
            $gte: new Date(),
          },
        },
      ];
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
      query.$and = [
        {
          $or: oldQuery,
        }, {
          $or: [
            {
              expires: null,
            }, {
              expires: {
                $gte: new Date().getTime(),
              },
            },
          ],
        },
      ];
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
    }
    // default sort added
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
        const aclQuery = [
          {
            $or: [
              {
                'ACL.read.allow.user': 'all',
              }, {
                'ACL.read.allow.user': accessList.userId,
              }, {
                'ACL.read.allow.role': {
                  $in: accessList.roles,
                },
              },
            ],
          }, {
            $and: [
              {
                'ACL.read.deny.user': {
                  $ne: accessList.userId,
                },
              }, {
                'ACL.read.deny.role': {
                  $nin: accessList.roles,
                },
              },
            ],
          },
        ];
        if (query.$and) query.$and.push({ $and: aclQuery });
        else query.$and = aclQuery;
      } else {
        query['ACL.read.allow.user'] = 'all';
      }
    }

    // check for include.
    if (query.$include) {
      if (query.$include.length > 0) {
        include = include.concat(query.$include);
      }
    }

    // delete $include and $includeList recursively
    query = this._sanitizeQuery(query);

    let findQuery = collection.find(query).project(select);

    if (Object.keys(sort).length > 0) {
      findQuery = findQuery.sort(sort);
    }

    if (skip) {
      if (Object.keys(sort).length === 0) { // default sort it in desc order on createdAt
        findQuery = findQuery.sort({ createdAt: -1 });
      }
      findQuery = findQuery.skip(skip);
    }

    findQuery = findQuery.limit(limit);

    return findQuery.toArray().then((docs) => {
      if (!include || include.length === 0) {
        docs = _deserialize(docs);

        return docs;
      }

      return this._include(appId, include, docs)
    });
  }

  /**
   *
   * @param {Object} details
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Object} details.query
   * @param {Object} details.select
   * @param {Object} details.sort
   * @param {number} details.limit
   * @param {number} details.skip
   * @param {Object} details.accessList
   * @param {boolean} details.isMasterKey
   *
   * @returns {Promise<Object|void>} document
   */
  findOne = async ({
    appId,
    collectionName,
    query,
    select,
    sort,
    skip,
    accessList,
    isMasterKey
  }) => {
    if (this.connected === false) {
      throw new errors.DatabaseConnectionError();
    }

    const documents = await this.find({
      appId,
      collectionName,
      query,
      select,
      sort,
      skip: 1,
      skip,
      accessList,
      isMasterKey
    })

    if (Array.isArray(documents) && documents.length > 0) {
      return documents[0];
    }
  }

  /**
   *
   * @param {Object} details
   * @param {string} details.appId
   * @param {Array} details.documents
   *
   * @returns {Promise<Object>} documents
   */
  save = async ({ appId, documents }) => {
    if (this.connected === false) {
      throw new errors.DatabaseConnectionError();
    }

    const savePromises = documents.map(document => {
      return this._save({
        appId,
        collectionName: document._tableName,
        document
      });
    });

    return Promise.all(savePromises);
  }

  /**
   *
   * @param {Object} details
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Object} details.document
   *
   * @returns {Promise<Object>} document
   */
  _update = async ({ appId, collectionName, document }) => {
    if (this.connected === false) {
      throw new errors.DatabaseConnectionError();
    }

    const collection = this.client
      .db(appId)
      .collection(util.collection.getId(appId, collectionName));


    const documentId = document._id;

    const query = {};
    query._id = documentId;

    return collection.updateOne(
      {
        _id: documentId,
      },
      document,
      {
        upsert: true,
      }
    );
  }

  /**
   *
   * @param {Object} details
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Object} details.query
   * @param {number} details.limit
   * @param {number} details.skip
   *
   * @returns {Promise<Object>} document
   */
  count = async ({ appId, collectionName, query, limit, skip }) => {
    if (this.connected === false) {
      throw new errors.DatabaseConnectionError();
    }

    const collection = this.client.db(appId)
      .collection(util.collection.getId(appId, collectionName));

    // delete $include and $includeList recursively
    const cleanQuery = this._sanitizeQuery(query);

    let findQuery = collection.find(cleanQuery);

    if (skip) {
      findQuery = findQuery.skip(parseInt(skip, 10));
    }

    return findQuery.count(cleanQuery);
  }

  /**
   *
   * @param {Object} details
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Array} details.onKey
   * @param {Object} details.query
   * @param {Object} details.select
   * @param {number} details.limit
   * @param {number} details.skip
   *
   * @returns {Promise<Object>} document
   */
  distinct = async ({
    appId,
    collectionName,
    onKey,
    query,
    select,
    sort,
    limit,
    skip
  }) => {
    if (this.connected === false) {
      throw new DatabaseConnectionError
    }

    const collection = this.client.db(appId)
      .collection(util.collection.getId(appId, collectionName));

    let include = [];

    if (query.$include) {
      if (query.$include.length > 0) {
        include = include.concat(query.$include);
      }
    }

    // delete $include and $includeList recursively
    query = this._sanitizeQuery(query);

    const keys = {};
    const indexForDot = onKey.indexOf('.');

    // if DOT in onKey
    //  keys = { beforeDot: { afterDot : "$beforeDot.afterDot"} }
    // else
    //  keys = { onKey : "$"+onKey }
    if (indexForDot !== -1) {
      // not using computed properties as it may not be available in server's nodejs version
      keys[onKey.slice(0, indexForDot)] = { };
      keys[onKey.slice(0, indexForDot)][onKey.slice(indexForDot + 1)] = `$${onKey}`;
    } else keys[onKey] = `$${onKey}`;

    if (!sort || Object.keys(sort).length === 0) sort = { createdAt: 1 };

    if (!query || Object.keys(query).length === 0) {
      query = {
        _id: {
          $exists: true,
        },
      };
    }

    const pipeline = [];
    pipeline.push({ $match: query });
    pipeline.push({ $sort: sort });

    // push the distinct aggregation.
    pipeline.push({
      $group: {
        _id: keys,
        document: {
          $first: '$$ROOT',
        },
      },
    });

    if (skip && skip !== 0) {
      pipeline.push({ $skip: skip });
    }

    if (limit && limit !== 0) {
      pipeline.push({ $limit: limit });
    }

    if (select && Object.keys(select).length > 0) {
      pipeline.push({
        $project: {
          document: select,
        },
      });
    }

    const cursor = await collection.aggregate(pipeline);
    const documents = await cursor.toArray();
    const includedDocuments = this._include(appId, include, documents);
    const deserializedDocuments = _deserialize(includedDocuments);

    return deserializedDocuments;
  }

  /**
   *
   * @param {Object} details
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Array} details.pipeline
   * @param {number} details.limit
   * @param {number} details.skip
   * @param {Object} details.accessList
   * @param {boolean} details.isMasterKey
   *
   * @returns {Promise<Object>}
   */
  aggregate = async ({
    appId,
    collectionName,
    pipeline,
    limit,
    skip,
    accessList,
    isMasterKey
  }) => {
    if (this.connected === false) {
      throw new errors.DatabaseConnectionError();
    }

    const collection = this.client.db(appId)
      .collection(mongoUtil.collection.getId(appId, collectionName));

    let query = {};
    if (pipeline.length > 0 && pipeline[0] && pipeline[0].$match) {
      query = pipeline[0].$match;
      pipeline.shift(); // remove first element.
    }

    if (!isMasterKey) {
      // if its not master key then apply ACL.
      if (accessList.userId) {
        const aclQuery = [
          {
            $or: [
              {
                'ACL.read.allow.user': 'all',
              }, {
                'ACL.read.allow.user': accessList.userId,
              }, {
                'ACL.read.allow.role': {
                  $in: accessList.roles,
                },
              },
            ],
          }, {
            $and: [
              {
                'ACL.read.deny.user': {
                  $ne: accessList.userId,
                },
              }, {
                'ACL.read.deny.role': {
                  $nin: accessList.roles,
                },
              },
            ],
          },
        ];
        if (query.$and) query.$and.push({ $and: aclQuery });
        else query.$and = aclQuery;
      } else {
        query['ACL.read.allow.user'] = 'all';
      }
    }

    if (!query.$or) {
      query.$or = [
        {
          expires: null,
        }, {
          expires: {
            $gte: new Date(),
          },
        },
      ];
    }

    pipeline.unshift({ $match: query }); // add item to the begining of the pipeline.

    if (skip && skip !== 0) {
      pipeline.push({ $skip: skip });
    }

    if (limit && limit !== 0) {
      pipeline.push({ $limit: limit });
    }

    return collection.aggregate(pipeline);
  }

  /**
   *
   *
   * @param {Object} details
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Object} details.document
   *
   * @returns {Promise}
   */
  _insert = async ({ appId, collectionName, document }) => {
    if (this.connected === false) {
      throw new errors.DatabaseConnectionError();
    }

    const collection = this.client.db(appId)
      .collection(mongoUtil.collection.getId(appId, collectionName));

    return collection.save(document);
  }

  /**
   *
   * @param {Object} details
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Object} details.document
   *
   * @returns {Promise}
   */
  delete = async ({ appId, collectionName, document }) => {
    const documentId = document._id;

    if (this.connected === false) {
      throw new errors.DatabaseConnectionError();
    }

    if (!document._id) {
      throw new errors.InvalidAccessError();
    }

    const collection = this.client.db(appId)
      .collection(mongoUtil.collection.getId(appId, collectionName));

    const query = {
      _id: documentId,
    };

    const doc = await collection.remove(query, {
      w: 1, // returns the number of documents removed
    });

    if (doc.result.n === 0) {
      throw new errors.WrongPermissionError();
    }

    return doc.result;
  }

  /**
   *
   * @param {Object} details
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Object} details.query
   *
   * @returns {Promise<Object>} result
   */
  deleteByQuery = async (appId, collectionName, query) => {
    if (this.connected === false) {
      throw new errors.DatabaseConnectionError();
    }

    const collection = this.client.db(appId)
      .collection(mongoUtil.collection.getId(appId, collectionName));

    const doc = await collection.remove(query, {
      w: 1, // returns the number of documents removed
    });

    return doc.result;
  }


  /**
   * Get file from gridfs
   *
   * @param {Object} details
   * @param {string} details.appId
   * @param {string} details.filename
   *
   * @returns {Promise<Object>} file
   */
  getFile = async ({ appId, filename }) => {
    if (this.connected === false) {
      throw new errors.DatabaseConnectionError();
    }

    return this.client.db(appId)
      .collection('fs.files')
      .findOne({
        filename,
      });
  }

  /**
   * Get fileStream from gridfs
   *
   * @param {Object} details
   * @param {string} details.appId
   * @param {string} details.fileId
   *
   * @returns {Filestream} filestream
   */
  getFileStreamById = ({ appId, fileId }) => {
    if (this.connected === false) {
      throw new errors.DatabaseConnectionError();
    }

    const gfs = Grid(this.client.db(appId), mongodb);
    const readstream = gfs.createReadStream({ _id: fileId });

    return readstream;
  }

  /**
   * Delete file from gridfs
   *
   * @param {Object} details
   * @param {string} details.appId
   * @param {string} details.filename
   *
   * @returns {Promise<boolean>}
   */
  deleteFileFromGridFs = async (appId, filename) => {
    if (this.connected === false) {
      throw new errors.DatabaseConnectionError();
    }

    const document = await this.client.db(appId)
      .collection('fs.files')
      .findOne({
        filename,
      });

    if (document) {
      const id = document._id;
      return config.mongoClient.db(appId)
        .collection('fs')
        .deleteMany({
          _id: id,
        });
    }

    throw new errors.FileDoesNotExistError();
  }

  /**
   * Save filestream to gridfs
   *
   * @param {Object} details
   * @param {string} appId
   * @param {Filestream} fileStream
   * @param {string} fileName
   * @param {string} contentType
   *
   * @returns {Promise<Object>} file
   */
  saveFileStream = ({
    appId,
    fileStream,
    fileName,
    contentType
  }) => {
    return new Promise((resolve, reject) => {
      try {
        const bucket = new mongodb.GridFSBucket(this.client.db(appId));
        const writeStream = bucket.openUploadStream(fileName, {
          contentType,
          w: 1,
        });

        fileStream.pipe(writeStream)
          .on('error', (err) => {
            writeStream.destroy();
            reject(err);
          })
          .on('finish', (file) => {
            resolve(file);
          });
      } catch (error) {
        reject(error);
      }
    })
  },

  /**
   * @param {Object} query
   *
   * @returns {Object} sanitizedQuery
   */
  _sanitizeQuery = (query) => {
    if (query) {
      const sanitizedQuery = JSON.parse(JSON.stringify(query));

      if (sanitizedQuery.$includeList) {
        delete sanitizedQuery.$includeList;
      }

      if (sanitizedQuery.$include) {
        delete sanitizedQuery.$include;
      }

      if (sanitizedQuery.$or && sanitizedQuery.$or.length > 0) {
        for (let i = 0; i < sanitizedQuery.$or.length; ++i) {
          sanitizedQuery.$or[i] = this._sanitizeQuery(sanitizedQuery.$or[i]);
        }
      }

      if (sanitizedQuery.$and && sanitizedQuery.$and.length > 0) {
        for (let i = 0; i < sanitizedQuery.$and.length; ++i) {
          sanitizedQuery.$and[i] = this._sanitizeQuery(sanitizedQuery.$and[i]);
        }
      }

      return sanitizedQuery;
    }
  }

  /**
   *
   * @param {Object} details
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {string} details.document
   *
   * @returns {Promise<Object>} document
   */
  _save = async ({ appId, collectionName, document }) => {
    const documentCopy = { ...document };

    if (documentCopy._isModified) {
      delete documentCopy._isModified;
    }
    if (document._modifiedColumns) {
      delete documentCopy._modifiedColumns;
    }

    const serializedDocument = this._serialize(documentCopy);

    // column key array to track sub documents.
    const document = await this._update({
      appId,
      collectionName,
      serializedDocument
    });

    const deserializedDocument = this._deserialize(document);

    return deserializedDocument;
  }

  /**
   * @param {Object} document
   *
   * @returns {Object} serializedDocument
   */
  _serialize = (document) => {
    const serializedDocument = JSON.parse(JSON.stringify(document));

    Object.keys(serializedDocument).forEach((key) => {
      if (serializedDocument[key]) {
        if (serializedDocument[key].constructor === Object && serializedDocument[key]._type) {
          if (serializedDocument[key]._type === 'point') {
            const _obj = {};
            _obj.type = 'Point';
            _obj.coordinates = document[key].coordinates;
            serializedDocument[key] = _obj;
          }
        }

        if (key === 'createdAt' || key === 'updatedAt' || key === 'expires') {
          if (typeof serializedDocument[key] === 'string') {
            serializedDocument[key] = new Date(serializedDocument[key]);
          }
        }
      }
    });

    return serializedDocument;
  }

  /**
   * @param {Array|Object} docs document or documents
   *
   * @returns {Array|Object} document or documents
   */
  _deserialize = (docs) => {
    if (docs.length > 0) {
      for (let i = 0; i < docs.length; i++) {
        const document = docs[i];
        const docKeys = Object.keys(document);
        for (let j = 0; j < docKeys.length; j++) {
          const key = docKeys[j];
          if (document[key]) {
            if (document[key].constructor === Object && document[key].type) {
              if (document[key].type === 'Point') {
                const _obj = {};
                _obj._type = 'point';
                _obj.coordinates = document[key].coordinates;
                _obj.latitude = _obj.coordinates[1]; // eslint-disable-line
                _obj.longitude = _obj.coordinates[0]; // eslint-disable-line
                document[key] = _obj;
              }
            } else if (document[key].constructor === Array
              && document[key][0]
              && document[key][0].type
              && document[key][0].type === 'Point') {
              const arr = [];
              for (let k = 0; k < document[key].length; k++) {
                const _obk = {};
                _obk._type = 'point';
                _obk.coordinates = document[key][k].coordinates;
                _obk.latitude = _obk.coordinates[1]; // eslint-disable-line
                _obk.longitude = _obk.coordinates[0]; // eslint-disable-line
                arr.push(_obk);
              }
              document[key] = arr;
            }
          }
        }
        docs[i] = document;
      }
    } else {
      const document = docs;
      const docKeys = Object.keys(document);
      for (let k = 0; k < docKeys.length; k++) {
        const key = docKeys[k];
        if (document[key]) {
          if (document[key].constructor === Object && document[key].type) {
            if (document[key].type === 'Point') {
              const _obj = {};
              _obj._type = 'point';
              _obj.coordinates = document[key].coordinates;
              _obj.latitude = _obj.coordinates[1]; // eslint-disable-line
              _obj.longitude = _obj.coordinates[0]; // eslint-disable-line
              document[key] = _obj;
            }
          } else if (document[key].constructor === Array
            && document[key][0]
            && document[key][0].type
            && document[key][0].type === 'Point') {
            const arr = [];
            for (let j = 0; j < document[key].length; j++) {
              const _obj = {};
              _obj._type = 'point';
              _obj.coordinates = document[key][j].coordinates;
              _obj.latitude = _obj.coordinates[1]; // eslint-disable-line
              _obj.longitude = _obj.coordinates[0]; // eslint-disable-line
              arr.push(_obj);
            }
            document[key] = arr;
          }
        }
      }
      docs = document;
    }

    return docs;
  }
}

export default MongoAdapter;
