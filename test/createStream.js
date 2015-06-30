var async = require('async'),
    BigNumber = require('bignumber.js'),
    helpers = require('./helpers')

var target = 'CreateStream',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertLimitExceeded = helpers.assertLimitExceeded.bind(null, target),
    assertInUse = helpers.assertInUse.bind(null, target)

describe('createStream', function() {

  describe('serializations', function() {

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
    })

    it('should return SerializationException when ShardCount is not an Integer', function(done) {
      assertType('ShardCount', 'Integer', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no StreamName', function(done) {
      assertValidation({},
        '2 validation errors detected: ' +
        'Value null at \'shardCount\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', ShardCount: 0},
        '3 validation errors detected: ' +
        'Value \'0\' at \'shardCount\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, ShardCount: 100000000000},
        '1 validation error detected: ' +
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128', done)
    })

    it('should return LimitExceededException for large ShardCount', function(done) {
      assertLimitExceeded({StreamName: randomName(), ShardCount: 1000},
        'This request would exceed the shard limit for the account ' + helpers.awsAccountId + ' in ' +
        helpers.awsRegion + '. Current shard count for the account: 3. Limit: ' + helpers.shardLimit + '. ' +
        'Number of additional shards that would have resulted from this request: 1000. ' +
        'Refer to the AWS Service Limits page (http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html) ' +
        'for current limits and how to request higher limits.', done)
    })

    it('should return ResourceInUseException if stream already exists', function(done) {
      assertInUse({StreamName: helpers.testStream, ShardCount: 1},
        'Stream ' + helpers.testStream + ' under account ' + helpers.awsAccountId + ' already exists.', done)
    })

  })

  describe('functionality', function() {

    it('should create a basic 3-shard stream', function(done) {
      this.timeout(100000)
      var stream = {StreamName: randomName(), ShardCount: 3}
      request(opts(stream), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        var createdAt = Date.now()

        res.body.should.equal('')

        request(helpers.opts('DescribeStream', stream), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          res.body.should.eql({
            StreamDescription: {
              StreamStatus: 'CREATING',
              StreamName: stream.StreamName,
              StreamARN: 'arn:aws:kinesis:' + helpers.awsRegion + ':' + helpers.awsAccountId +
                ':stream/' + stream.StreamName,
              HasMoreShards: false,
              Shards: [],
            },
          })

          async.parallel([
            helpers.assertNotFound.bind(helpers, 'GetShardIterator',
              {StreamName: stream.StreamName, ShardId: 'shardId-0', ShardIteratorType: 'LATEST'},
              'Shard shardId-000000000000 in stream ' + stream.StreamName + ' under account ' + helpers.awsAccountId + ' does not exist'),
            helpers.assertInUse.bind(helpers, 'MergeShards',
              {StreamName: stream.StreamName, ShardToMerge: 'shardId-0', AdjacentShardToMerge: 'shardId-1'},
              'Stream ' + stream.StreamName + ' under account ' + helpers.awsAccountId + ' not ACTIVE, instead in state CREATING'),
            helpers.assertNotFound.bind(helpers, 'PutRecord',
              {StreamName: stream.StreamName, PartitionKey: 'a', Data: ''},
              'Stream ' + stream.StreamName + ' under account ' + helpers.awsAccountId + ' not found.'),
            helpers.assertNotFound.bind(helpers, 'PutRecords',
              {StreamName: stream.StreamName, Records: [{PartitionKey: 'a', Data: ''}]},
              'Stream ' + stream.StreamName + ' under account ' + helpers.awsAccountId + ' not found.'),
            helpers.assertInUse.bind(helpers, 'SplitShard',
              {StreamName: stream.StreamName, ShardToSplit: 'shardId-0', NewStartingHashKey: '2'},
              'Stream ' + stream.StreamName + ' under account ' + helpers.awsAccountId + ' not ACTIVE, instead in state CREATING'),
          ], function(err) {
            if (err) return done(err)

            helpers.waitUntilActive(stream.StreamName, function(err, res) {
              if (err) return done(err)

              res.body.StreamDescription.Shards[0].SequenceNumberRange.StartingSequenceNumber.should.match(/^\d{56}$/)
              res.body.StreamDescription.Shards[1].SequenceNumberRange.StartingSequenceNumber.should.match(/^\d{56}$/)
              res.body.StreamDescription.Shards[2].SequenceNumberRange.StartingSequenceNumber.should.match(/^\d{56}$/)

              var startSeq0 = new BigNumber(res.body.StreamDescription.Shards[0].SequenceNumberRange.StartingSequenceNumber),
                startSeq1 = new BigNumber(res.body.StreamDescription.Shards[1].SequenceNumberRange.StartingSequenceNumber),
                startSeq2 = new BigNumber(res.body.StreamDescription.Shards[2].SequenceNumberRange.StartingSequenceNumber)

              startSeq1.minus(startSeq0).toFixed().should.equal('22300745198530623141535718272648361505980432')
              startSeq2.minus(startSeq1).toFixed().should.equal('22300745198530623141535718272648361505980432')

              var startDiff = parseInt(startSeq0.toString(16).slice(2, 10), 16) - (createdAt / 1000)
              startDiff.should.be.below(-2)
              startDiff.should.be.above(-7)

              delete res.body.StreamDescription.Shards[0].SequenceNumberRange.StartingSequenceNumber
              delete res.body.StreamDescription.Shards[1].SequenceNumberRange.StartingSequenceNumber
              delete res.body.StreamDescription.Shards[2].SequenceNumberRange.StartingSequenceNumber

              res.body.should.eql({
                StreamDescription: {
                  StreamStatus: 'ACTIVE',
                  StreamName: stream.StreamName,
                  StreamARN: 'arn:aws:kinesis:' + helpers.awsRegion + ':' + helpers.awsAccountId +
                    ':stream/' + stream.StreamName,
                  HasMoreShards: false,
                  Shards: [{
                    ShardId: 'shardId-000000000000',
                    SequenceNumberRange: {},
                    HashKeyRange: {
                      StartingHashKey: '0',
                      EndingHashKey: '113427455640312821154458202477256070484',
                    },
                  }, {
                    ShardId: 'shardId-000000000001',
                    SequenceNumberRange: {},
                    HashKeyRange: {
                      StartingHashKey: '113427455640312821154458202477256070485',
                      EndingHashKey: '226854911280625642308916404954512140969',
                    },
                  }, {
                    ShardId: 'shardId-000000000002',
                    SequenceNumberRange: {},
                    HashKeyRange: {
                      StartingHashKey: '226854911280625642308916404954512140970',
                      EndingHashKey: '340282366920938463463374607431768211455',
                    },
                  }],
                },
              })

              request(helpers.opts('ListStreams', {Limit: 1}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)

                res.body.StreamNames.should.have.length(1)
                res.body.HasMoreStreams.should.equal(true)

                request(helpers.opts('DeleteStream', {StreamName: stream.StreamName}), done)
              })
            })
          })
        })
      })
    })
  })

})
