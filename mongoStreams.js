var _ = require('lodash');
var async = require('async');
var MongoClient = require('mongodb').MongoClient;
var parseMongoUri = require('mongo-uri').parse;
var mongoStream = require('./mongoStream');

function listCollections(mongoUri, callback) {
  return async.waterfall([
    function(callback) {
      return MongoClient.connect(mongoUri, callback);
    },
    function(db, callback) {
      return db
        .listCollections()
        .toArray(_.partial(callback, _, db));
    },
    function(db, collections, callback) {
      db.close();
      return callback(null, _.map(collections, function(collection) {
        return collection.name;
      }));
    }
  ], callback);
}

function streamCollection(uriInfo, collectionName, callback) {
  return async.waterfall([
    function(callback) {
      return mongoStream(uriInfo, collectionName, callback);
    },
    function(dumpStream, callback) {
      return callback(null, {
        name: collectionName,
        stream: dumpStream
      });
    }
  ], callback);
}

module.exports = function mongoStreams(mongoUri, callback) {
  return async.waterfall([
    function(callback) {
      return listCollections(mongoUri, callback);
    },
    function(collectionNames, callback) {
      var uriInfo = parseMongoUri(mongoUri);
      return async.map(collectionNames, _.partial(streamCollection, uriInfo), callback);
    }
  ], callback);
};
