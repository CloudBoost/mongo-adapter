class DatabaseConnectionError extends Error {
  constructor (...params) {
    super(...params);

    this.name = 'DatabaseConnectionError';
    this.code = 400;
    this.message = 'Database not connected';
  }
}

class InvalidAccessError extends Error {
  constructor (...params) {
    super(...params);

    this.name = 'InvalidAccessError';
    this.code = 400;
    this.message = 'You cant delete an unsaved object';
  }
}

class WrongPermissionError extends Error {
  constructor (...params) {
    super(...params);

    this.name = 'WrongPermissionError';
    this.code = 401;
    this.message = 'You do not have permission to delete';
  }
}

class FileDoesNotExistError extends Error {
  constructor (...params) {
    super(...params);

    this.name = 'FileDoesNotExistError';
    this.code = 404;
    this.message = 'File does not exist';
  }
}

export default {
  DatabaseConnectionError,
  InvalidAccessError,
  WrongPermissionError,
  FileDoesNotExistError
};
