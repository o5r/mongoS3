var spawn = require('child_process').spawn;

function createDumpArgs(uriInfo, collection) {
  var args = [];

  // Host
  var host = uriInfo['ports'][0] ? uriInfo['hosts'][0] + ':' + uriInfo['ports'][0] : uriInfo['hosts'][0];
  args.push('-h');
  args.push(host);

  // Auth
  if (uriInfo['username']) {
    args.push('-u');
    args.push(uriInfo['username']);
    args.push('-p');
    args.push(uriInfo['password']);
  }

  // Database
  args.push('-d');
  args.push(uriInfo['database']);

  // Collection
  args.push('-c');
  args.push(collection);

  // Output (stdout)
  args.push('-o');
  args.push('-');

  return args;
}

module.exports = function mongoStream(uriInfo, collectionName, callback) {
  var args = createDumpArgs(uriInfo, collectionName);

  var dumpChild = spawn('mongodump', args);
  dumpChild.on('error', callback);
  dumpChild.stdout.pause();
  return callback(null, dumpChild.stdout);
};
