import { ConnectionStringParser } from 'connection-string-parser';
import mongodb from 'mongodb';
import errors from './errors';
import BaseAdaptor from 'data-adaptor-base';

const { ReplSet, Server, MongoClient, Grid, Db } = mongodb;

class MongoAdapter extends BaseAdaptor {
  /**
   * Creates a MongoDB connection
   *
   * @param {Object} credentials credentials object
   * @param {string} credentials.connectionString MongoDB connection url
   *
   * @returns {Promise<Object>} dbClient
   */
  static connect = async ({ connectionString }) => {
    const client = await MongoClient.connect(connectionString, {
      poolSize: 200,
      useNewUrlParser: true,
    });

    return client;
  }

  /**
   *
   * @param {Object} configs
   * @param {string} connectionString database connection string
   *
   * @returns {ReplSet} replSet
   */
  static _replSet = (configs, connectionString = '') => {
    if (Array.isArray(configs) && configs.length > 0) {
      const servers = configs.map(config => {
        return new Server(config.host, parseInt(config.port, 10))
      });

      const replSet = new ReplSet(servers);

      return replSet;
    }
    const connectionStringParser = new ConnectionStringParser({
      scheme: "mongodb",
      hosts: []
    });

    const connectionObject = connectionStringParser
      .parse(connectionString);

    return new ReplSet(connectionObject.hosts);
  }


  /**
   * Disconnects current MongoDB connection or throws an error
   * if no connection exists
   *
   * @memberof Connector
   *
   * @param {Object} client Connected mongo database client
   *
   * @returns {void}
   */
  static disconnect = client => {
    client.close();
  }

  /**
   * Returns the document that matches the _id with the documentId
   *
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {string} details.documentId
   * @param {Object} details.accessList
   * @param {boolean} details.isMasterKey
   *
   * @returns {Promise<object>} document
   */
  static findById = async ({
    client,
    appId,
    collectionName,
    documentId,
    accessList,
    isMasterKey
  }) => {
    return MongoAdapter.findOne({
      client,
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
  static _include = async (appId, include, docs) => {
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

      promises.push(MongoAdapter._fetch_data(appId, collectionName, query));
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
          pr.push(MongoAdapter._include(appId, rInclude, arrayOfDocs[i]));
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

        const deserializedDocs = MongoAdapter.deserialize(currentDocs);

        return deserializedDocs;
      });
    });
  }

  /**
   * @param {Object} mongoClient
   * @param {string} appId
   * @param {string} collectionName
   * @param {Object} qry
   *
   * @returns {Promise<Array>} documents
   */
  static _fetch_data = async (client, appId, collectionName, qry) => {
    if (!collectionName || !qry._id.$in) {
      return [];
    }

    return client.db(appId)
      .collection(collectionName)
      .find(qry)
      .toArray();
  }

  /**
   *
   * @param {Object} details
   * @param {Object} details.client
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
  static find = async ({
    client,
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
    const collection = client
      .db(appId)
      .collection(collectionName);

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
      let oldQuery = query.$or;
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
    query = MongoAdapter._sanitizeQuery(query);

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

    const documents = await findQuery.toArray();

    if (!include || include.length === 0) {
      const deserializedDocuments = MongoAdapter.deserialize(documents);

      return deserializedDocuments;
    }

    return MongoAdapter._include({ appId, include, documents })
  }

  /**
   *
   * @param {Object} details
   * @param {Object} details.client
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
  static findOne = async ({
    client,
    appId,
    collectionName,
    query,
    select,
    sort,
    skip,
    accessList,
    isMasterKey
  }) => {
    const documents = await MongoAdapter.find({
      client,
      appId,
      collectionName,
      query,
      select,
      sort,
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
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {Array} details.documents
   *
   * @returns {Promise<Object>} documents
   */
  static save = async ({ client, appId, documents }) => {
    const savePromises = documents.map(document => {
      return MongoAdapter._save({
        client,
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
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Object} details.document
   *
   * @returns {Promise<Object>} document
   */
  static _update = async ({ client, appId, collectionName, document }) => {
    const collection = client
      .db(appId)
      .collection(collectionName);


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
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Object} details.query
   * @param {number} details.skip
   *
   * @returns {Promise<Object>} document
   */
  static count = async ({
    client,
    appId,
    collectionName,
    query,
    skip
  }) => {

    const collection = client.db(appId)
      .collection(collectionName);

    // delete $include and $includeList recursively
    const cleanQuery = MongoAdapter._sanitizeQuery(query);

    let findQuery = collection.find(cleanQuery);

    if (skip) {
      findQuery = findQuery.skip(parseInt(skip, 10));
    }

    return findQuery.count(cleanQuery);
  }

  /**
   *
   * @param {Object} details
   * @param {Object} details.client
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
  static distinct = async ({
    client,
    appId,
    collectionName,
    onKey,
    query,
    select,
    sort,
    limit,
    skip
  }) => {
    const collection = client.db(appId)
      .collection(collectionName);

    let include = [];

    if (query.$include) {
      if (query.$include.length > 0) {
        include = include.concat(query.$include);
      }
    }

    // delete $include and $includeList recursively
    query = MongoAdapter._sanitizeQuery(query);

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
    const includedDocuments = MongoAdapter._include(appId, include, documents);
    const deserializedDocuments = MongoAdapter.deserialize(includedDocuments);

    return deserializedDocuments;
  }

  /**
   *
   * @param {Object} details
   * @param {Object} details.client
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
  static aggregate = async ({
    client,
    appId,
    collectionName,
    pipeline,
    limit,
    skip,
    accessList,
    isMasterKey
  }) => {
    const collection = client.db(appId)
      .collection(collectionName);

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
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Object} details.document
   *
   * @returns {Promise}
   */
  static _insert = async ({
    client,
    appId,
    collectionName,
    document
  }) => {
    return client.db(appId)
      .collection(collectionName)
      .save(document);
  }

  /**
   *
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Object} details.document
   *
   * @returns {Promise}
   */
  static delete = async ({
    client,
    appId,
    collectionName,
    document
  }) => {
    const documentId = document._id;

    if (!document._id) {
      throw new errors.InvalidAccessError();
    }

    const collection = client.db(appId)
      .collection(collectionName);

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
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Object} details.query
   *
   * @returns {Promise<Object>} result
   */
  static deleteByQuery = async ({
    client,
    appId,
    collectionName,
    query
  }) => {
    const collection = client.db(appId)
      .collection(collectionName);

    const doc = await collection.remove(query, {
      w: 1, // returns the number of documents removed
    });

    return doc.result;
  }


  /**
   * Get file from gridfs
   *
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.filename
   *
   * @returns {Promise<Object>} file
   */
  static getFile = async ({
    client,
    appId,
    filename
  }) => {
    return client.db(appId)
      .collection('fs.files')
      .findOne({
        filename,
      });
  }

  /**
   * Get fileStream from gridfs
   *
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.fileId
   *
   * @returns {Filestream} filestream
   */
  static getFileStreamById = ({
    client,
    appId,
    fileId
  }) => {
    const gfs = Grid(client.db(appId), mongodb);
    const readstream = gfs.createReadStream({ _id: fileId });

    return readstream;
  }

  /**
   * Delete file from gridfs
   *
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.filename
   *
   * @returns {Promise<boolean>}
   */
  static deleteFileFromGridFs = async ({ client, appId, filename }) => {
    const document = await client.db(appId)
      .collection('fs.files')
      .findOne({
        filename,
      });

    if (document) {
      const id = document._id;
      return client.db(appId)
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
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {Filestream} details.fileStream
   * @param {string} details.fileName
   * @param {string} details.contentType
   *
   * @returns {Promise<Object>} file
   */
  static saveFileStream = ({
    client,
    appId,
    fileStream,
    fileName,
    contentType
  }) => {
    return new Promise((resolve, reject) => {
      try {
        const bucket = new mongodb.GridFSBucket(client.db(appId));
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
  }

  /**
   * @param {Object} query
   *
   * @returns {Object} sanitizedQuery
   */
  static _sanitizeQuery = (query) => {
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
          sanitizedQuery.$or[i] = MongoAdapter._sanitizeQuery(sanitizedQuery.$or[i]);
        }
      }

      if (sanitizedQuery.$and && sanitizedQuery.$and.length > 0) {
        for (let i = 0; i < sanitizedQuery.$and.length; ++i) {
          sanitizedQuery.$and[i] = MongoAdapter._sanitizeQuery(sanitizedQuery.$and[i]);
        }
      }

      return sanitizedQuery;
    }
  }

  /**
   *
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {string} details.document
   *
   * @returns {Promise<Object>} document
   */
  static _save = async ({ client, appId, collectionName, document }) => {
    const documentCopy = { ...document };

    if (documentCopy._isModified) {
      delete documentCopy._isModified;
    }
    if (document._modifiedColumns) {
      delete documentCopy._modifiedColumns;
    }

    const serializedDocument = MongoAdapter.serialize(documentCopy);

    // column key array to track sub documents.
    const updatedDocument = await MongoAdapter._update({
      client,
      appId,
      collectionName,
      serializedDocument
    });

    const deserializedDocument = MongoAdapter.deserialize(updatedDocument);

    return deserializedDocument;
  }

  /**
   * @param {Object} document
   *
   * @returns {Object} serializedDocument
   */
  static serialize = (document) => {
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
  static deserialize = (docs) => {
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

  /**
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {string} details.indexString
   *
   * @returns {Promise<Object|undefined>}
   */
  static _dropIndex = async ({
    client,
    appId,
    collectionName,
    indexString
  }) => {
    if (indexString && indexString !== '') {
      const collection = client.db(appId).collection(collectionName);

      return collection.dropIndex(indexString);
    }
  }

   /**
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Object} details.query
   *
   * @returns {Promise<Object|undefined>}
   */
  static _unsetColumn = async ({
    client,
    appId,
    collectionName,
    query
  }) => {
    if (query && Object.keys(query).length > 0) {
      const collection = client.db(appId).collection(collectionName);

      return collection.update({}, {
        $unset: query,
      }, {
        multi: true,
      });
    }
  }


  /**
   *
   * @param {Object} details
   * @param {string} details.appId
   * @param {Object} details.replSet
   *
   * @return {Promise<Object>} db
   */
  static createDatabase = async ({ appId, replSet }) => {
    const db = new Db(appId, replSet, { w: 1 });

    return db;
  }

  /**
   *
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   *
   * @returns {Promise}
   */
  static deleteDatabase ({ client, appId }) {
    const database = client.db(appId);

    return database.dropDatabase();
  }

  /**
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Object} details.column
   *
   * @returns {Promise}
   */
  static addColumn({ client, appId, collectionName, column }) {
    if (column.dataType === 'GeoPoint' || column.dataType === 'Text') {
      return MongoAdapter.createIndex({
        client,
        appId,
        collectionName,
        columnName: column.name,
        columnType: column.dataType
      });
    }
  }

  /**
   *
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {Array} details.schema
   *
   * @returns {Promise}
   */
  static createCollection = async ({ client, appId, collectionName, schemas }) => {
    for (let i = 0; i < schemas.length; i++) {
      if (schemas[i].dataType === 'GeoPoint') {
        await MongoAdapter.createIndex({
          client,
          appId,
          collectionName,
          columnName: schemas[i].name,
          columnType: schemas[i].dataType
        });
      }
    }
  }

  /**
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {string} details.columnType
   *
   * @returns {Promise}
   */
  static createIndex = async ({
    client,
    appId,
    collectionName,
    columnName,
    columnType
  }) => {
    const obj = {};

    if (columnType === 'Text') {
      obj['$**'] = 'text';
    }

    if (columnType === 'GeoPoint') {
      obj[columnName] = '2dsphere';
    }

    if (Object.keys(obj).length > 0) {
      return client.db(appId)
        .collection(collectionName)
        .createIndex(obj);
    }
  }

  /**
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   *
   * @returns {Promise}
   */
  static deleteAndCreateTextIndexes = async ({ client, appId, collectionName }) => {
    return client.db(appId)
      .collection(collectionName)
      .createIndex({
        '$**': 'text',
      });
  }

  /**
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   *
   * @returns {Promise}
   */
  static getIndexes = async ({ client, appId, collectionName }) => {
    return client.db(appId)
      .collection(collectionName)
      .indexInformation();
  }


  /**
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {string} details.oldColumnName
   * @param {string} details.newColumnName
   *
   * @return {Promise}
   */
  static renameColumn = async ({
    client,
    appId,
    collectionName,
    oldColumnName,
    newColumnName
  }) => {
    const collection = client.db(appId)
      .collection(collectionName);

    const query = {};

    query[oldColumnName] = newColumnName;

    return collection.update(
      {},
      {
        $rename: query,
      },
      {
        multi: true,
      }
    );
  }

  /**
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   * @param {string} details.columnName
   * @param {string} details.columnType
   *
   * @returns {Promise}
   */
  static deleteColumn = async ({
    client,
    appId,
    collectionName,
    columnName,
    columnType
  }) => {
    const query = {};

    query[columnName] = 1;

    let indexName = null;

    if (columnType === 'GeoPoint') {
      indexName = `${columnName}_2dsphere`;
    }

    await MongoAdapter._dropIndex({ client, appId, collectionName, indexName });
    await MongoAdapter._unsetColumn({ client, appId, collectionName, query });
  }

  /**
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.collectionName
   *
   * @returns {Promise}
   */
  static deleteTable = async ({ client, appId, collectionName }) => {
    return client.db(appId)
      .collection(collectionName)
      .drop();
  }

  /**
   * @param {Object} details
   * @param {Object} details.client
   * @param {string} details.appId
   * @param {string} details.oldCollectionName
   * @param {string} details.newCollectionName
   *
   * @returns {Promise}
   */
  static renameTable = async ({
    client,
    appId,
    oldCollectionName,
    newCollectionName
  }) => {
    return client.db(appId)
      .collection(oldCollectionName)
      .rename(newCollectionName);
  }
}

export default MongoAdapter;
