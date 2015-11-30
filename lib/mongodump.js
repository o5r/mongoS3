var AWS = require('aws-sdk');
var Promise = require('bluebird');
var archiver = require('archiver');
var mongoStreams = require('./mongo-streams');

/**
 * @param {Object} opts
 * @param {Object} opts.s3
 * @param {Object} opts.s3.accessKeyId
 * @param {Object} opts.s3.secretAccessKey
 * @param {Object} opts.s3.bucket
 * @param {Object} opts.s3.key
 * @param {Stream} stream
 * @returns {bluebird}
 */
function uploadStreamToS3(opts, stream) {
  return new Promise(function (resolve, reject) {
    var s3 = new AWS.S3({
      params: {
        Bucket: opts.s3.bucket,
        Key: opts.s3.key
      }
    });
    s3.upload({ Body: stream }).send(function (err) {
      if (err) {
        return reject(err);
      }
      return resolve();
    });
  });
}

/**
 * @returns {bluebird}
 */
function createArchiveStream() {
  return new Promise(function(resolve, reject) {
    var archiveStream = archiver('zip');
    archiveStream.on('error', reject);
    return resolve(archiveStream);
  });
}

/**
 * @param {Object} opts
 * @param {String} opts.uri
 * @param {Object} opts.s3
 * @param {String} opts.s3.accessKeyId
 * @param {String} opts.s3.secretAccessKey
 * @param {String} opts.s3.bucket
 * @param {String} opts.s3.key
 */
module.exports = function mongodump(opts) {
  return Promise.all([
    mongoStreams(opts),
    createArchiveStream()
  ]).spread(function(collectionsStreams, archiveStream) {
    return Promise.all([archiveStream, Promise.each(collectionsStreams, function (collectionStream) {
      archiveStream.append(collectionStream.stream, { name: collectionStream.name + '.bson' });
    })]);
  }).spread(function (archiveStream) {
    archiveStream.finalize();
    return uploadStreamToS3(opts, archiveStream);
  }).catch(console.error);
};
