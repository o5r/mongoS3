var spawn = require('child_process').spawn;

var MongoURI = require('mongo-uri');
var Promise = require('bluebird');
var fs = require('fs');
var os = require('os');
var path = require('path');

/**
 * @param {Object} opts
 * @param {String} opts.collection
 * @param {String} [opts.uri]
 * @param {Array<String>} [opts.ports]
 * @param {Array<String>} [opts.hosts]
 * @param {String} [opts.username]
 * @param {String} [opts.password]
 * @param {String} [opts.database]
 * @returns {Array}
 */
function createDumpArgs(opts) {
  var args = [];
  var _hostInfo = opts.uri ? MongoURI.parse(opts.uri) : opts;
  // We assume we connect to the first instance
  var host = _hostInfo.ports[0] ? _hostInfo.hosts[0] + ':' + _hostInfo.ports[0] : _hostInfo.hosts[0];

  if (host) {
    args.push('-h', host);
  }
  if (_hostInfo.username) {
    args.push('-u', _hostInfo.username);
  }
  if (_hostInfo.password) {
    args.push('-p', _hostInfo.password);
  }
  args.push('-d', _hostInfo.database);
  args.push('-c', opts.collection);
  // Output (stdout)
  args.push('-o', '-');

  return args;
}

/**
 * @param opts
 * @param {string} opts.uri
 * @param {string} opts.collection
 * @param {Function} [_log]
 * @returns {bluebird}
 */
module.exports = function spawnMongodump(opts, _log) {
  var log = _log === undefined ? console.log : _log;

  return new Promise(function(resolve, reject) {
    var args = createDumpArgs(opts);
    var fileName = opts.collection + '.bson';
    var output = path.join(os.tmpDir(), fileName);
    var mongodump = spawn('mongodump', args);

    mongodump.stdout.pipe(fs.createWriteStream(output));
    if (typeof log === 'function') {
      mongodump.stderr.on('data', function(data) {
        log(data.toString('ascii'), opts.collection);
      });
    }
    mongodump.on('error', reject);

    return mongodump.stdout.on('end', function() {
      return resolve({
        name: fileName,
        path: output
      });
    });
  });
};
