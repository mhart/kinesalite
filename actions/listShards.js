
module.exports = function listShards(store, data, cb) {
  // https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html
  // Currently we don't support the ExclusiveStartShardId and NextToken requests.
  store.getStream(data.StreamName, function(err, stream) {
    if (err) return cb(err)

    cb(null, {Shards: stream.Shards})
  })
}