let db = require('../db'),
    BigNumber = require('bignumber.js'),
    POW_128 = new BigNumber(2).pow(128),
    SEQ_ADJUST_MS = 2000

module.exports = function(shardCount, startIndex) {

  startIndex = startIndex || 0

  var i
  let shards = new Array(shardCount)
  let shardHash = POW_128.div(shardCount).integerValue(BigNumber.ROUND_FLOOR)
  let createTime = Date.now() - SEQ_ADJUST_MS
  for (i = 0; i < shardCount; i++) {
    shards[i] = {
      HashKeyRange: {
        StartingHashKey: shardHash.times(i).toFixed(),
        EndingHashKey: (i < shardCount - 1 ? shardHash.times(i + 1) : POW_128).minus(1).toFixed(),
      },
      SequenceNumberRange: {
        StartingSequenceNumber: db.stringifySequence({shardCreateTime: createTime, shardIx: i}),
      },
      ShardId: db.shardIdName(startIndex + i),
    }
  }

  return shards
}
