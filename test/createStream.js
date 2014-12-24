var should = require('should'),
    helpers = require('./helpers')

var target = 'CreateStream',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertLimitExceeded = helpers.assertLimitExceeded.bind(null, target)

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
        'This request would exceed the shard limit for the account ' + helpers.awsAccountId + ' in us-east-1. ' +
        'Current shard count for the account: 0. Limit: 10. ' +
        'Number of additional shards that would have resulted from this request: 1000. ' +
        'Refer to the AWS Service Limits page (http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html) ' +
        'for current limits and how to request higher limits.', done)
    })

  })

  describe('functionality', function() {

    it('should create a basic 2-shard stream', function(done) {
      this.timeout(100000)
      var stream = {StreamName: randomName(), ShardCount: 2}
      request(opts(stream), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        res.body.should.equal('')

        request(helpers.opts('DescribeStream', stream), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          res.body.StreamDescription.StreamStatus.should.equal('CREATING')
          res.body.StreamDescription.StreamName.should.equal(stream.StreamName)
          res.body.StreamDescription.StreamARN.should.equal('arn:aws:kinesis:us-east-1:' + helpers.awsAccountId + ':stream/' + stream.StreamName)
          res.body.StreamDescription.Shards.should.be.empty
          res.body.StreamDescription.HasMoreShards.should.be.false

          helpers.waitUntilActive(stream.StreamName, function(err, res) {
            if (err) return done(err)

            res.body.StreamDescription.StreamStatus.should.equal('ACTIVE')
            res.body.StreamDescription.Shards.should.have.length(2)

            res.body.StreamDescription.Shards[0].ShardId.should.equal('shardId-000000000000')
            res.body.StreamDescription.Shards[0].SequenceNumberRange.StartingSequenceNumber.should.have.length(56)
            res.body.StreamDescription.Shards[0].SequenceNumberRange.StartingSequenceNumber.should.match(/^\d+$/)
            res.body.StreamDescription.Shards[0].HashKeyRange.StartingHashKey.should.equal('0')
            res.body.StreamDescription.Shards[0].HashKeyRange.EndingHashKey.should.equal('170141183460469231731687303715884105727')

            res.body.StreamDescription.Shards[1].ShardId.should.equal('shardId-000000000001')
            res.body.StreamDescription.Shards[1].SequenceNumberRange.StartingSequenceNumber.should.have.length(56)
            res.body.StreamDescription.Shards[1].SequenceNumberRange.StartingSequenceNumber.should.match(/^\d+$/)
            res.body.StreamDescription.Shards[1].HashKeyRange.StartingHashKey.should.equal('170141183460469231731687303715884105728')
            res.body.StreamDescription.Shards[1].HashKeyRange.EndingHashKey.should.equal('340282366920938463463374607431768211455')

            // Eg, for shard index 1 of a total of 3
            // Big.RM = 0 (round down)
            // Big('340282366920938463463374607431768211456').div(3).times(1).toFixed(0) to
            // Big('340282366920938463463374607431768211456').div(3).times(2).minus(1).toFixed(0)

            // Eg hash ranges for 3 shards:
            // shard 0: 0 - 113427455640312821154458202477256070484
            // shard 1: 113427455640312821154458202477256070485 - 226854911280625642308916404954512140969
            // shard 2: 226854911280625642308916404954512140970 - 340282366920938463463374607431768211455

            // Eg hash ranges for 4 shards:
            // shard 0: 0 - 85070591730234615865843651857942052863
            // shard 1: 85070591730234615865843651857942052864 - 170141183460469231731687303715884105727
            // shard 2: 170141183460469231731687303715884105728 - 255211775190703847597530955573826158591
            // shard 3: 255211775190703847597530955573826158592 - 340282366920938463463374607431768211455

            done()
          })
        })
      })
    })
  })

})

