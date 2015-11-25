var AWS = require('aws-sdk');
var Promise = require('bluebird');
var archiver = require('archiver');
var s3UploadStream = require('s3-upload-stream');
var mongoStreams = require('./mongo-streams');

/**
 * @param {Object} opts
 * @param {Object} opts.S3
 * @param {Object} opts.S3.accessKeyId
 * @param {Object} opts.S3.secretAccessKey
 * @param {Object} opts.S3.Bucket
 * @param {Object} opts.S3.Key
 * @returns {bluebird}
 */
function createUploadStream(opts) {
  return new Promise(function(resolve, reject) {
    var uploadStream = s3UploadStream(new AWS.S3({
      accessKeyId: opts.S3.accessKeyId,
      secretAccessKey: opts.S3.secretAccessKey
    })).upload({
      'Bucket': opts.S3.Bucket,
      'Key': opts.S3.Key
    });
    uploadStream.on('error', reject);
    return resolve(uploadStream);
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
 * @param {String} opts.URI
 * @param {Object} opts.S3
 * @param {String} opts.S3.accessKeyId
 * @param {String} opts.S3.secretAccessKey
 * @param {String} opts.S3.Bucket
 * @param {String} opts.S3.Key
 */
module.exports = function mongodump(opts) {
  return Promise.all([
    mongoStreams(opts),
    createUploadStream(opts),
    createArchiveStream()
  ]).spread(function(collectionsStreams, uploadStream, archiveStream) {
    archiveStream.pipe(uploadStream);

    return Promise.all([
      archiveStream,
      uploadStream,
      Promise.each(collectionsStreams, function(collectionStream) {
        return archiveStream.append(collectionStream.stream, { name: collectionStream.name + '.bson' });
      })
    ]);
  }).spread(function(archiveStream, uploadStream) {
    archiveStream.finalize();

    return new Promise(function(resolve) {
      return uploadStream.on('uploaded', resolve);
    });
  });
};
