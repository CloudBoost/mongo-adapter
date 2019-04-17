const winston = require('winston');
const { ConnectionStringParser } = require('connection-string-parser')
const { ReplSet, Server, MongoClient } = require('mongodb');

const connectionStringParser = new ConnectionStringParser({
  scheme: "mongodb",
  hosts: []
});

const connector = {
  client: null,

  connected: false,

  connectionString: '',

  connect: async (connectionString) => {
    try {
      const dbClient = await MongoClient.connect(connectionString, {
        poolSize: 200,
        useNewUrlParser: true,
      });

      this.client = dbClient;
      this.connected = true;
      this.connectionString = connectionString;

      return dbClient
    } catch (error) {
      winston.log('error', error)
    }
  },

  dbConnect: (appId) => {
    if (this.connected) {
      return this.client.db(appId)
    }

    throw new Error('Database not connected');
  },

  replSet(configs) {
    try {
      if (Array.isArray(configs) && configs.length > 0) {
        const servers = configs.map(config => {
          return new Server(config.host, parseInt(mongoConfig.port, 10))
        });

        const replSet = new ReplSet(servers);

        return replSet;
      }

      const connectionObject = connectionStringParser.parse(connector.connectionString);

      return new ReplSet(connectionObject.hosts);

      
    } catch (error) {
      winston.log(
        'error',
        {
          error: error.message,
          stack: error.stack
        }
      );
    }
  },

  close: () => {
    if (this.connected) {
      this.client.close();
      this.connected = false;
    } else {
      throw new Error('Database not connected');
    }
  },
}

module.exports = connector;
