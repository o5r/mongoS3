var mongodump = require('../index');

describe('mongudump test suite', function () {
    this.timeout(10000);
    it('work', function (done) {
        return mongodump({
            URI: process.env.MONGO_URI,
            S3: {
                accessKeyId: process.env.AWS_ACCESS_KEY_ID,
                secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
                Bucket: process.env.AWS_S3_DUMP_BUCKET,
                Key: process.env.AWS_S3_DUMP_KEY
            }
        }).then(function () {
            done();
        }).catch(done);
    });
});