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
 * @param {*} stream
 * @returns {bluebird}
 */
function uploadStreamToS3(opts, stream) {
  var s3 = new AWS.S3({
    params: {
      Bucket: opts.s3.bucket,
      Key: opts.s3.key
    }
  });
  var upload = s3.upload({ Body: stream });

  return Promise.promisify(upload.send, { context: upload })();
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
  var archive = archiver.create('zip', {});

  return mongoStreams(opts)
    .each(function(file) {
      return archive.append(fs.createReadStream(file.path), { name: file.name });
    }).then(function() {
      archive.finalize();
      return uploadStreamToS3({
        s3: {
          accessKeyId: opts.s3.accessKeyId,
          secretAccessKey: opts.s3.secretAccessKey,
          bucket: opts.s3.bucket,
          key: opts.s3.key
        }
      }, archive);
    });
};
