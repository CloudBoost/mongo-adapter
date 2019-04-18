class DatabaseConnectionError extends Error {
  constructor (...params) {
    super(...params);

    this.code = 400;
    this.message = 'Database not connected';
  }
}

module.exports = {
  DatabaseConnectionError
}
