var db = require('../db'), createShards = require('./util.createShards')

module.exports = function createStream(store, data, cb) {

  var key = data.StreamName, metaDb = store.metaDb

  metaDb.lock(key, function(release) {
    cb = release(cb)

    metaDb.get(key, function(err) {
      if (err && err.name != 'NotFoundError') return cb(err)
      if (!err)
        return cb(db.clientError('ResourceInUseException',
          'Stream ' + key + ' under account ' + metaDb.awsAccountId + ' already exists.'))

      db.sumShards(store, function(err, shardSum) {
        if (err) return cb(err)

        if (shardSum + data.ShardCount > store.shardLimit) {
          return cb(db.clientError('LimitExceededException',
              'This request would exceed the shard limit for the account ' + metaDb.awsAccountId + ' in ' +
              metaDb.awsRegion + '. Current shard count for the account: ' + shardSum +
              '. Limit: ' + store.shardLimit + '. Number of additional shards that would have ' +
              'resulted from this request: ' + data.ShardCount + '. Refer to the AWS Service Limits page ' +
              '(http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html) ' +
              'for current limits and how to request higher limits.'))
        }

        stream = {
          RetentionPeriodHours: 24,
          EnhancedMonitoring: [{ShardLevelMetrics: []}],
          HasMoreShards: false,
          Shards: [],
          StreamARN: 'arn:aws:kinesis:' + metaDb.awsRegion + ':' + metaDb.awsAccountId + ':stream/' + data.StreamName,
          StreamName: data.StreamName,
          StreamStatus: 'CREATING',
          _seqIx: new Array(Math.ceil(data.ShardCount / 5)), // Hidden data, remove when returning
          _tags: Object.create(null), // Hidden data, remove when returning
        }

        var shards = createShards(data.ShardCount)

        metaDb.put(key, stream, function(err) {
          if (err) return cb(err)

          setTimeout(function() {

            // Shouldn't need to lock/fetch as nothing should have changed
            stream.StreamStatus = 'ACTIVE'
            stream.Shards = shards

            metaDb.put(key, stream, function(err) {
              if (err && !/Database is not open/.test(err)) console.error(err.stack || err)
            })

          }, store.createStreamMs)

          cb()
        })
      })
    })
  })

}
