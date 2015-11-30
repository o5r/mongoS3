var AWS = require('aws-sdk');
var Promise = require('bluebird');
var archiver = require('archiver');
var mongoStreams = require('./mongo-streams');
var fs = require('fs');
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
 * @param {Object} opts
 * @param {String} opts.uri
 * @param {Object} opts.s3
 * @param {String} opts.s3.accessKeyId
 * @param {String} opts.s3.secretAccessKey
 * @param {String} opts.s3.bucket
 * @param {String} opts.s3.key
 */
module.exports = function mongodump(opts) {
  return mongoStreams(opts)
    .map(function (filename) {
      return Promise.all([fs.createReadStream(filename.stream), filename]);
    }).each(function (readStream) {
        return uploadStreamToS3({
          s3: {
            accessKeyId: opts.s3.accessKeyId,
            secretAccessKey: opts.s3.secretAccessKey,
            bucket: opts.s3.bucket,
            key: opts.s3.key + readStream[1].name + '.bson'
          }
        }, readStream[0]);
      }).catch(console.error);
};
