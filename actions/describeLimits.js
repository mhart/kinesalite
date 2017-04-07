var db = require('../db')

module.exports = function describeLimits(store, data, cb) {

  db.sumShards(store, function(err, shardSum) {
    if (err) return cb(err)

    cb(null, {OpenShardCount: shardSum, ShardLimit: store.shardLimit})
  })
}
