var BigNumber = require('bignumber.js'),
    helpers = require('./helpers'),
    db = require('../db')

var target = 'UpdateShardCount',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target),
    assertInUse = helpers.assertInUse.bind(null, target),
    assertLimitExceeded = helpers.assertLimitExceeded.bind(null, target)

describe('updateShardCount', function() {

  describe('serializations', function() {

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
    })

    it('should return SerializationException when ScalingType is not a String', function(done) {
      assertType('ScalingType', 'String', done)
    })

    it('should return SerializationException when TargetShardCount is not an Integer', function(done) {
      assertType('TargetShardCount', 'Integer', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no StreamName', function(done) {
      assertValidation({}, [
        'Value null at \'scalingType\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'targetShardCount\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', ScalingType: '', TargetShardCount: 0}, [
        'Value \'\' at \'scalingType\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [UNIFORM_SCALING]',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'0\' at \'targetShardCount\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, ScalingType: 'UNIFORM_SCALING', TargetShardCount: 1}, [
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128',
      ], done)
    })

    it('should return ValidationException for large TargetShardCount', function(done) {
      assertValidation({StreamName: randomName(), ScalingType: 'UNIFORM_SCALING', TargetShardCount: 100001}, [
        'Value \'100001\' at \'targetShardCount\' failed to satisfy constraint: ' +
        'Member must have value less than or equal to 100000',
      ], done)
    })

    it('should return ResourceNotFoundException if stream does not exist', function(done) {
      var name = randomName()
      assertNotFound({StreamName: name, ScalingType: 'UNIFORM_SCALING', TargetShardCount: 2},
        'Stream ' + name + ' under account ' + helpers.awsAccountId + ' not found.', done)
    })

    it('should return LimitExceededException if increasing over shards limit', function(done) {
      this.timeout(100000)
      var stream = {StreamName: randomName(), ShardCount: helpers.shardLimit / 5}

      request(helpers.opts('CreateStream', stream), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        helpers.waitUntilActive(stream.StreamName, function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          var targetShardCount = helpers.shardLimit + 2

          assertLimitExceeded({StreamName: stream.StreamName, TargetShardCount: targetShardCount, ScalingType: "UNIFORM_SCALING"},
            'Target shard count or number of open shards cannot be greater than ' + helpers.shardLimit + '. ' +
            'Current open shard count: 10, Target shard count: ' + targetShardCount, function(err) {
            if (err) return done(err)

            request(helpers.opts('DeleteStream', {StreamName: stream.StreamName}), done)
          })
        })
      })
    })

    it('should return LimitExceededException if scaling above double open shards', function(done) {
      this.timeout(100000)
      var stream = {StreamName: randomName(), ShardCount: 2}

      request(helpers.opts('CreateStream', stream), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        helpers.waitUntilActive(stream.StreamName, function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          var targetShardCount = 6

          assertLimitExceeded({StreamName: stream.StreamName, TargetShardCount: targetShardCount, ScalingType: "UNIFORM_SCALING"},
            'UpdateShardCount cannot scale up over double your current open shard count. ' +
            'Current open shard count: 2, Target shard count: ' + targetShardCount, function(err) {
            if (err) return done(err)

            request(helpers.opts('DeleteStream', {StreamName: stream.StreamName}), done)
          })
        })
      })
    })

    it('should return LimitExceededException if scaling down below half open shards', function(done) {
      this.timeout(100000)
      var stream = {StreamName: randomName(), ShardCount: 6}

      request(helpers.opts('CreateStream', stream), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        helpers.waitUntilActive(stream.StreamName, function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          var targetShardCount = 2

          assertLimitExceeded({StreamName: stream.StreamName, TargetShardCount: targetShardCount, ScalingType: "UNIFORM_SCALING"},
            'UpdateShardCount cannot scale down below half your current open shard count. ' +
            'Current open shard count: 6, Target shard count: ' + targetShardCount, function(err) {
            if (err) return done(err)

            request(helpers.opts('DeleteStream', {StreamName: stream.StreamName}), done)
          })
        })
      })
    })
  })

  describe('functionality', function() {

    it('should increase open stream shards to 4', function(done) {
      this.timeout(100000)
      var stream = {StreamName: randomName(), ShardCount: 2}

      request(helpers.opts('CreateStream', stream), function(err, res) {
        if (err) return done(err)

        helpers.waitUntilActive(stream.StreamName, function(err, res) {

          if (err) return done(err)

          request(helpers.opts('UpdateShardCount', {
            StreamName: stream.StreamName,
            TargetShardCount: 4,
            ScalingType: 'UNIFORM_SCALING',
          }), function(err, res) {
            if (err) return done(err)

            res.statusCode.should.equal(200)
            res.body.StreamName.should.equal(stream.StreamName)
            res.body.CurrentShardCount.should.equal(2)
            res.body.TargetShardCount.should.equal(4)

            request(helpers.opts('DescribeStream', stream), function(err, res) {

              if (err) return done(err)
              res.statusCode.should.equal(200)

              res.body.StreamDescription.StreamStatus.should.eql('UPDATING');
              res.body.StreamDescription.Shards.length.should.eql(2);

              helpers.waitUntilActive(stream.StreamName, function(err, res) {

                var shards = res.body.StreamDescription.Shards,
                    closedShards = shards.filter(function(shard) {
                      return shard.SequenceNumberRange.EndingSequenceNumber != null
                    }).length

                shards.length.should.eql(6)
                closedShards.should.eql(2)
                request(helpers.opts('DeleteStream', {StreamName: stream.StreamName}), done)
              })
            })
          })
        })
      })
    })

    it('should decrease open stream shards to 2', function(done) {
      this.timeout(100000)
      var stream = {StreamName: randomName(), ShardCount: 4}

      request(helpers.opts('CreateStream', stream), function(err, res) {
        if (err) return done(err)

        helpers.waitUntilActive(stream.StreamName, function(err, res) {

          if (err) return done(err)

          request(helpers.opts('UpdateShardCount', {
            StreamName: stream.StreamName,
            TargetShardCount: 2,
            ScalingType: 'UNIFORM_SCALING',
          }), function(err, res) {
            if (err) return done(err)

            res.statusCode.should.equal(200)
            res.body.should.equal('')

            request(helpers.opts('DescribeStream', stream), function(err, res) {

              if (err) return done(err)
              res.statusCode.should.equal(200)

              res.body.StreamDescription.StreamStatus.should.eql('UPDATING');
              res.body.StreamDescription.Shards.length.should.eql(4);

              helpers.waitUntilActive(stream.StreamName, function(err, res) {

                var shards = res.body.StreamDescription.Shards,
                    closedShards = shards.filter(function(shard) {
                      return shard.SequenceNumberRange.EndingSequenceNumber != null
                    }).length

                shards.length.should.eql(6)
                closedShards.should.eql(4)
                request(helpers.opts('DeleteStream', {StreamName: stream.StreamName}), done)
              })
            })
          })
        })
      })
    })
  })
})
