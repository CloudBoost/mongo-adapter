const mongodb = require('mongodb');
const winston = require('winston');
const { ReplSet, Server, MongoClient } = require('mongodb');

class MongoAdapter {
  constructor (connectionString) {
    this.client = null;
    this.connectionString = connectionString;
  }

  async connect () {
    try {
      return MongoClient.connect(this.connectionString, {
        poolSize: 200,
        useNewUrlParser: true,
      });

    } catch (error) {
      winston.log('error', {
        error: String(e),
        stack: error.stack
      });
      return error
    }
  }
}

module.exports = MongoAdapter;


/*
  mongoClient = MongoAdapter.connect()
*/
