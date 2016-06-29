var BigNumber = require('bignumber.js'),
    helpers = require('./helpers'),
    db = require('../db')

var target = 'SplitShard',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target),
    assertInUse = helpers.assertInUse.bind(null, target),
    assertLimitExceeded = helpers.assertLimitExceeded.bind(null, target)

describe('splitShard', function() {

  describe('serializations', function() {

    it('should return SerializationException when NewStartingHashKey is not an String', function(done) {
      assertType('NewStartingHashKey', 'String', done)
    })

    it('should return SerializationException when ShardToSplit is not a String', function(done) {
      assertType('ShardToSplit', 'String', done)
    })

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no StreamName', function(done) {
      assertValidation({}, [
        'Value null at \'newStartingHashKey\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'shardToSplit\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', ShardToSplit: '', NewStartingHashKey: ''}, [
        'Value \'\' at \'newStartingHashKey\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: 0|([1-9]\\d{0,38})',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'\' at \'shardToSplit\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'shardToSplit\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, ShardToSplit: name, NewStartingHashKey: '0'}, [
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128',
        'Value \'' + name + '\' at \'shardToSplit\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128',
      ], done)
    })

    it('should return ResourceNotFoundException if unknown stream and shard ID just small enough', function(done) {
      var name1 = randomName(), name2 = '2147483647'
      assertNotFound({StreamName: name1, ShardToSplit: name2, NewStartingHashKey: '0'},
        'Stream ' + name1 + ' under account ' + helpers.awsAccountId + ' not found.', done)
    })

    it('should return ResourceNotFoundException if unknown stream and random shard name', function(done) {
      var name1 = randomName(), name2 = randomName() + '-2147483647'
      assertNotFound({StreamName: name1, ShardToSplit: name2, NewStartingHashKey: '0'},
        'Stream ' + name1 + ' under account ' + helpers.awsAccountId + ' not found.', done)
    })

    it('should return ResourceNotFoundException if unknown stream and short prefix', function(done) {
      var name1 = randomName(), name2 = 'a-00002147483647'
      assertNotFound({StreamName: name1, ShardToSplit: name2, NewStartingHashKey: '0'},
        'Stream ' + name1 + ' under account ' + helpers.awsAccountId + ' not found.', done)
    })

    it('should return ResourceNotFoundException if unknown stream and no prefix', function(done) {
      var name1 = randomName(), name2 = '-00002147483647'
      assertNotFound({StreamName: name1, ShardToSplit: name2, NewStartingHashKey: '0'},
        'Stream ' + name1 + ' under account ' + helpers.awsAccountId + ' not found.', done)
    })

    it('should return ResourceNotFoundException if unknown stream and shard ID too big', function(done) {
      var name1 = randomName(), name2 = '2147483648'
      assertNotFound({StreamName: name1, ShardToSplit: name2, NewStartingHashKey: '0'},
        'Could not find shard ' + name2 + ' in stream ' + name1 + ' under account ' + helpers.awsAccountId + '.', done)
    })

    it('should return ResourceNotFoundException if unknown stream and raw shard ID too big', function(done) {
      var name1 = randomName(), name2 = 'shardId-002147483648'
      assertNotFound({StreamName: name1, ShardToSplit: name2, NewStartingHashKey: '0'},
        'Could not find shard ' + name2 + ' in stream ' + name1 + ' under account ' + helpers.awsAccountId + '.', done)
    })

    it('should return ResourceNotFoundException if unknown stream and string shard ID', function(done) {
      var name1 = randomName(), name2 = 'ABKLFD8'
      assertNotFound({StreamName: name1, ShardToSplit: name2, NewStartingHashKey: '0'},
        'Could not find shard ' + name2 + ' in stream ' + name1 + ' under account ' + helpers.awsAccountId + '.', done)
    })

    it('should return ResourceNotFoundException if unknown stream and exponent shard ID', function(done) {
      var name1 = randomName(), name2 = '2.14E4'
      assertNotFound({StreamName: name1, ShardToSplit: name2, NewStartingHashKey: '0'},
        'Could not find shard ' + name2 + ' in stream ' + name1 + ' under account ' + helpers.awsAccountId + '.', done)
    })

    it('should return ResourceNotFoundException if known stream and raw shard ID does not exist', function(done) {
      assertNotFound({StreamName: helpers.testStream, ShardToSplit: 'shardId-5', NewStartingHashKey: '0'},
        'Could not find shard shardId-000000000005 in stream ' + helpers.testStream +
          ' under account ' + helpers.awsAccountId + '.', done)
    })

    it('should return InvalidArgumentException for hash too small', function(done) {
      assertInvalidArgument({
        StreamName: helpers.testStream,
        NewStartingHashKey: '1',
        ShardToSplit: 'shard-0',
      }, 'NewStartingHashKey 1 used in SplitShard() on shard ' +
        'shardId-000000000000 in stream ' + helpers.testStream + ' under account ' + helpers.awsAccountId +
        ' is not both greater than one plus the shard\'s StartingHashKey 0 and less than the shard\'s ' +
        'EndingHashKey 113427455640312821154458202477256070484.', done)
    })

    it('should return InvalidArgumentException for hash too big', function(done) {
      assertInvalidArgument({
        StreamName: helpers.testStream,
        NewStartingHashKey: '113427455640312821154458202477256070484',
        ShardToSplit: 'shard-0',
      }, 'NewStartingHashKey 113427455640312821154458202477256070484 used in SplitShard() on shard ' +
        'shardId-000000000000 in stream ' + helpers.testStream + ' under account ' + helpers.awsAccountId +
        ' is not both greater than one plus the shard\'s StartingHashKey 0 and less than the shard\'s ' +
        'EndingHashKey 113427455640312821154458202477256070484.', done)
    })

    it('should return InvalidArgumentException for hash much too big', function(done) {
      assertInvalidArgument({
        StreamName: helpers.testStream,
        NewStartingHashKey: '999999999999999999999999999999999999999',
        ShardToSplit: 'shard-2',
      }, 'NewStartingHashKey 999999999999999999999999999999999999999 used in SplitShard() on shard ' +
        'shardId-000000000002 in stream ' + helpers.testStream + ' under account ' + helpers.awsAccountId +
        ' is not both greater than one plus the shard\'s StartingHashKey 226854911280625642308916404954512140970 ' +
        'and less than the shard\'s EndingHashKey 340282366920938463463374607431768211455.', done)
    })

    // Is a bit too fragile to run with everything else – just run alone
    it.skip('should return LimitExceededException if splitting over limit', function(done) {
      this.timeout(100000)
      var stream = {StreamName: randomName(), ShardCount: helpers.shardLimit - 3}
      request(helpers.opts('CreateStream', stream), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        helpers.waitUntilActive(stream.StreamName, function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          assertLimitExceeded({StreamName: stream.StreamName, NewStartingHashKey: '2', ShardToSplit: 'shard-0'},
              'This request would exceed the shard limit for the account ' + helpers.awsAccountId + ' in ' +
              helpers.awsRegion + '. Current shard count for the account: ' + helpers.shardLimit + '. Limit: ' +
              helpers.shardLimit + '. Number of additional shards that would have resulted from this request: 1. ' +
              'Refer to the AWS Service Limits page ' +
              '(http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html) ' +
              'for current limits and how to request higher limits.', function(err) {
            if (err) return done(err)

            request(helpers.opts('DeleteStream', {StreamName: stream.StreamName}), done)
          })
        })
      })
    })

  })

  describe('functionality', function() {

    // Takes 65 secs to run on production
    it('should work with minimum hash', function(done) {
      this.timeout(100000)
      var stream = {StreamName: randomName(), ShardCount: 1}
      request(helpers.opts('CreateStream', stream), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        helpers.waitUntilActive(stream.StreamName, function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          var splitStart = Date.now()

          request(opts({
            StreamName: stream.StreamName,
            NewStartingHashKey: '2',
            ShardToSplit: randomName() + '-0',
          }), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)

            res.body.should.equal('')

            request(helpers.opts('DescribeStream', stream), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)

              delete res.body.StreamDescription.Shards[0].SequenceNumberRange.StartingSequenceNumber

              res.body.should.eql({
                StreamDescription: {
                  StreamStatus: 'UPDATING',
                  StreamName: stream.StreamName,
                  StreamARN: 'arn:aws:kinesis:' + helpers.awsRegion + ':' + helpers.awsAccountId +
                    ':stream/' + stream.StreamName,
                  RetentionPeriodHours: 24,
                  EnhancedMonitoring: [{ShardLevelMetrics: []}],
                  HasMoreShards: false,
                  Shards: [{
                    ShardId: 'shardId-000000000000',
                    SequenceNumberRange: {},
                    HashKeyRange: {
                      StartingHashKey: '0',
                      EndingHashKey: '340282366920938463463374607431768211455',
                    },
                  }],
                },
              })

              assertInUse({StreamName: stream.StreamName, NewStartingHashKey: '2', ShardToSplit: 'shard-0'},
                  'Stream ' + stream.StreamName + ' under account ' + helpers.awsAccountId +
                  ' not ACTIVE, instead in state UPDATING', function(err) {
                if (err) return done(err)

                request(helpers.opts('PutRecord', {
                  StreamName: stream.StreamName,
                  PartitionKey: 'a',
                  Data: '',
                  ExplicitHashKey: '1',
                }), function(err, res) {
                  if (err) return done(err)
                  res.statusCode.should.equal(200)

                  res.body.ShardId.should.equal('shardId-000000000000')

                  helpers.waitUntilActive(stream.StreamName, function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)

                    var splitEnd = Date.now()

                    var shards = res.body.StreamDescription.Shards

                    shards.should.have.length(3)

                    var seq0Start = db.parseSequence(shards[0].SequenceNumberRange.StartingSequenceNumber)
                    var seq0End = db.parseSequence(shards[0].SequenceNumberRange.EndingSequenceNumber)
                    var seq1Start = db.parseSequence(shards[1].SequenceNumberRange.StartingSequenceNumber)
                    var seq2Start = db.parseSequence(shards[2].SequenceNumberRange.StartingSequenceNumber)

                    seq0End.shardIx.should.equal(0)
                    seq0End.shardCreateTime.should.equal(seq0Start.shardCreateTime)
                    new BigNumber(seq0End.seqIx).toString(16).should.equal('7fffffffffffffff')
                    seq0End.seqTime.should.be.above(splitStart - 1000)
                    seq0End.seqTime.should.be.below(splitEnd + 1000)

                    seq1Start.shardIx.should.equal(1)
                    seq1Start.shardCreateTime.should.equal(seq1Start.seqTime)

                    seq2Start.shardIx.should.equal(2)
                    seq2Start.shardCreateTime.should.equal(seq2Start.seqTime)

                    seq1Start.shardCreateTime.should.equal(seq2Start.shardCreateTime)

                    var diff = seq1Start.shardCreateTime - seq0End.seqTime
                    diff.should.equal(1000)

                    seq1Start.seqIx.should.equal('0')
                    seq2Start.seqIx.should.equal('0')

                    delete res.body.StreamDescription.Shards[0].SequenceNumberRange.StartingSequenceNumber
                    delete res.body.StreamDescription.Shards[0].SequenceNumberRange.EndingSequenceNumber
                    delete res.body.StreamDescription.Shards[1].SequenceNumberRange.StartingSequenceNumber
                    delete res.body.StreamDescription.Shards[2].SequenceNumberRange.StartingSequenceNumber

                    res.body.should.eql({
                      StreamDescription: {
                        StreamStatus: 'ACTIVE',
                        StreamName: stream.StreamName,
                        StreamARN: 'arn:aws:kinesis:' + helpers.awsRegion + ':' + helpers.awsAccountId +
                          ':stream/' + stream.StreamName,
                        RetentionPeriodHours: 24,
                        EnhancedMonitoring: [{ShardLevelMetrics: []}],
                        HasMoreShards: false,
                        Shards: [{
                          ShardId: 'shardId-000000000000',
                          SequenceNumberRange: {},
                          HashKeyRange: {
                            StartingHashKey: '0',
                            EndingHashKey: '340282366920938463463374607431768211455',
                          },
                        }, {
                          ShardId: 'shardId-000000000001',
                          ParentShardId: 'shardId-000000000000',
                          SequenceNumberRange: {},
                          HashKeyRange: {
                            StartingHashKey: '0',
                            EndingHashKey: '1',
                          },
                        }, {
                          ShardId: 'shardId-000000000002',
                          ParentShardId: 'shardId-000000000000',
                          SequenceNumberRange: {},
                          HashKeyRange: {
                            StartingHashKey: '2',
                            EndingHashKey: '340282366920938463463374607431768211455',
                          },
                        }],
                      },
                    })

                    putRecord(done)

                    function putRecord(cb) {
                      request(helpers.opts('PutRecord', {
                        StreamName: stream.StreamName,
                        PartitionKey: 'a',
                        Data: '',
                        ExplicitHashKey: '1',
                      }), function(err, res) {
                        if (err) return cb(err)
                        res.statusCode.should.equal(200)

                        var putSeq = db.parseSequence(res.body.SequenceNumber)

                        // Kinesis will continue to put records in the "closed" shard
                        // until the time has clocked over to the new shard's create time
                        if (res.body.ShardId == 'shardId-000000000000') {
                          helpers.assertSequenceNumber(res.body.SequenceNumber, 0, splitEnd)
                          putSeq.shardCreateTime.should.equal(seq0End.shardCreateTime)
                          putSeq.seqTime.should.be.below(seq0End.seqTime + 1)
                          return putRecord(cb)
                        }

                        res.body.ShardId.should.equal('shardId-000000000001')
                        helpers.assertSequenceNumber(res.body.SequenceNumber, 1, splitEnd)

                        putSeq.shardCreateTime.should.equal(seq1Start.shardCreateTime)
                        putSeq.seqTime.should.be.above(putSeq.shardCreateTime - 1)

                        checkHorizonShardIterator(cb)
                      })
                    }

                    function checkHorizonShardIterator(cb) {
                      request(helpers.opts('GetShardIterator', {
                        StreamName: stream.StreamName,
                        ShardId: 'shardId-000000000000',
                        ShardIteratorType: 'TRIM_HORIZON',
                      }), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)

                        Object.keys(res.body).should.eql(['ShardIterator'])
                        helpers.assertShardIterator(res.body.ShardIterator, stream.StreamName)

                        request(helpers.opts('GetRecords', {
                          ShardIterator: res.body.ShardIterator,
                        }), function(err, res) {
                          if (err) return done(err)
                          res.statusCode.should.equal(200)

                          // Sometimes returns with a NextShardIterator, sometimes doesn't
                          if (res.body.NextShardIterator) {
                            helpers.assertShardIterator(res.body.NextShardIterator, stream.StreamName)
                            delete res.body.NextShardIterator
                          }

                          Object.keys(res.body).sort().should.eql(['MillisBehindLatest', 'Records'])
                          res.body.MillisBehindLatest.should.be.within(0, 10000)
                          res.body.Records.should.not.be.empty()

                          checkLatestShardIterator(cb)
                        })
                      })
                    }

                    function checkLatestShardIterator(retries, cb) {
                      if (!cb) { cb = retries; retries = 0 }
                      if (retries >= 10) return cb(new Error('Too many retries trying to call GetRecords'))

                      request(helpers.opts('GetShardIterator', {
                        StreamName: stream.StreamName,
                        ShardId: 'shardId-000000000000',
                        ShardIteratorType: 'LATEST',
                      }), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)

                        Object.keys(res.body).should.eql(['ShardIterator'])
                        helpers.assertShardIterator(res.body.ShardIterator, stream.StreamName)

                        request(helpers.opts('GetRecords', {
                          ShardIterator: res.body.ShardIterator,
                        }), function(err, res) {
                          if (err) return done(err)
                          res.statusCode.should.equal(200)

                          // Can continue to return a number of times with a NextShardIterator after shard is closed
                          if (res.body.NextShardIterator) return checkLatestShardIterator(++retries, cb)

                          Object.keys(res.body).sort().should.eql(['MillisBehindLatest', 'Records'])
                          res.body.Records.should.eql([])
                          res.body.MillisBehindLatest.should.be.within(0, 10000)

                          request(helpers.opts('DeleteStream', {StreamName: stream.StreamName}), cb)
                        })
                      })
                    }

                  })
                })
              })
            })
          })
        })
      })
    })

  })

})
