var _ = require('lodash');
var async = require('async');
var AWS = require('aws-sdk');
var archiver = require('archiver');
var s3UploadStream = require('s3-upload-stream');
var mongoStreams = require('./mongoStreams');

module.exports = function mongoDump(mongoUri, s3, path, callback) {
  return async.waterfall([
    function(callback) {
      return mongoStreams(mongoUri, callback);
    },
    function(collectionStreams, callback) {
      var uploadStream = s3UploadStream(new AWS.S3({
        accessKeyId: s3.accessKeyId,
        secretAccessKey: s3.secretAccessKey
      })).upload({
        'Bucket': s3.bucket,
        'Key': path
      });
      uploadStream.on('error', callback);
      uploadStream.on('uploaded', function(entry) {
        console.log('uploadStream.on(uploaded)');
        return callback(null, entry.Key);
      });

      var archiveStream = archiver('zip');
      archiveStream.on('error', callback);

      archiveStream.pipe(uploadStream);

      _.each(collectionStreams, function(collectionStream) {
        collectionStream.stream.on('error', function() {
          console.error('collectionStream_' + collectionStream.name + '.on(close):', arguments);
        });
        collectionStream.stream.on('close', function() {
          console.log('collectionStream_' + collectionStream.name + '.on(close): ' + archiveStream.pointer() + ' total bytes');
        });

        archiveStream.append(collectionStream.stream, { name: collectionStream.name + '.bson' });
      });

      archiveStream.finalize();
    }
  ], callback);
};
