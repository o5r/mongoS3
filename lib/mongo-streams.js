var Promise = require('bluebird');
var mongodb = Promise.promisifyAll(require('mongodb'));

var spawnMongodump = require('./spawn-mongodump');

var MongoClient = mongodb.MongoClient;

/**
 * @param {Object} opts
 * @param {String} opts.uri
 * @returns {bluebird}
 */
function listCollections(opts) {
  return MongoClient.connectAsync(opts.uri)
    .then(function(db) {
      return Promise.all([
        db,
        db.listCollections().toArray()
      ]);
    }).spread(function(db, collections) {
      db.close();
      return collections;
    }).map(function(collection) {
      return collection.name;
    });
}

/**
 * @param {Object} opts
 * @param {String} opts.uri
 * @returns {bluebird}
 */
module.exports = function mongoStreams(opts) {
  return listCollections(opts)
    .map(function(collection) {
      return spawnMongodump({ uri: opts.uri, collection: collection });
    });
};
