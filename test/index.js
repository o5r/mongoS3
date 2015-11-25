var mongodump = require('../index');

describe('mongudump test suite', function () {
    this.timeout(10000);
    it('work', function (done) {
        return mongodump({
            uri: process.env.MONGO_URI,
            s3: {
                accessKeyId: process.env.AWS_ACCESS_KEY_ID,
                secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
                bucket: process.env.AWS_S3_DUMP_BUCKET,
                key: process.env.AWS_S3_DUMP_KEY
            }
        }).then(function () {
            done();
        }).catch(done);
    });
});
