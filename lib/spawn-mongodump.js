var spawn = require('child_process').spawn;

var MongoURI = require('mongo-uri');
var Promise = require('bluebird');

/**
 * @param {Object} opts
 * @param {String} opts.collection
 * @param {String} [opts.URI]
 * @param {Array<String>} [opts.ports]
 * @param {Array<String>} [opts.hosts]
 * @param {String} [opts.username]
 * @param {String} [opts.password]
 * @param {String} [opts.database]
 * @returns {Array}
 */
function createDumpArgs (opts) {
  var args = [];
  var _hostInfo = opts.URI ? MongoURI.parse(opts.URI) : opts;
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
  args.push('-c', _hostInfo.collection);
  // Output (stdout)
  args.push('-o', '-');

  return args;
}

/**
 * @param opts
 * @param opts.URI
 * @param opts.collection
 * @returns {bluebird}
 */
module.exports = function spawnMongodump (opts) {
  return new Promise(function (resolve, reject) {
    var args = createDumpArgs(opts);
    var mongodump = spawn('mongodump', args);

    mongodump.on('error', reject);
    mongodump.stdout.pause();

    return resolve(mongodump.stdout);
  });
};
