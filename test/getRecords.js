var should = require('should'),
    helpers = require('./helpers')

var target = 'GetRecords',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target)

describe('getRecords', function() {

  describe('serializations', function() {

    it('should return SerializationException when Limit is not an Integer', function(done) {
      assertType('Limit', 'Integer', done)
    })

    it('should return SerializationException when ShardIterator is not a String', function(done) {
      assertType('ShardIterator', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no ShardIterator', function(done) {
      assertValidation({},
        '1 validation error detected: ' +
        'Value null at \'shardIterator\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty ShardIterator', function(done) {
      assertValidation({ShardIterator: '', Limit: 0},
        '2 validation errors detected: ' +
        'Value \'0\' at \'limit\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1; ' +
        'Value \'\' at \'shardIterator\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for long ShardIterator', function(done) {
      var name = new Array(513 + 1).join('a')
      assertValidation({ShardIterator: name, Limit: 100000},
        '2 validation errors detected: ' +
        'Value \'100000\' at \'limit\' failed to satisfy constraint: ' +
        'Member must have value less than or equal to 10000; ' +
        'Value \'' + name + '\' at \'shardIterator\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 512', done)
    })

    it('should return InvalidArgumentException if ShardIterator is incorrect format', function(done) {
      var name = randomName()
      assertInvalidArgument({ShardIterator: name}, 'Invalid ShardIterator.', done)
    })

    // Takes 5 minutes to run
    it.skip('should return ExpiredIteratorException if ShardIterator has expired', function(done) {
      this.timeout(310000)
      request(helpers.opts('GetShardIterator', {
        StreamName: helpers.testStream,
        ShardId: 'shardId-0',
        ShardIteratorType: 'LATEST',
      }), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        setTimeout(function() {
          request(opts({ShardIterator: res.body.ShardIterator}), function(err, res) {
            if (err) return done(err)

            res.statusCode.should.equal(400)
            res.body.__type.should.equal('ExpiredIteratorException')
            res.body.message.should.match(new RegExp('^Iterator expired\\. ' +
              'The iterator was created at time \\w{3} \\w{3} \\d{2} \\d{2}:\\d{2}:\\d{2} UTC \\d{4} ' +
              'while right now it is \\w{3} \\w{3} \\d{2} \\d{2}:\\d{2}:\\d{2} UTC \\d{4} ' +
              'which is further in the future than the tolerated delay of 300000 milliseconds\\.$'))

            done()
          })
        }, 300000)
      })
    })

    // Takes 95 secs to run on production
    it('should return ResourceNotFoundException if shard or stream does not exist', function(done) {
      this.timeout(200000)
      var stream = {StreamName: randomName(), ShardCount: 2}
      request(helpers.opts('CreateStream', stream), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        helpers.waitUntilActive(stream.StreamName, function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          request(helpers.opts('GetShardIterator', {
            StreamName: stream.StreamName,
            ShardId: 'shardId-0',
            ShardIteratorType: 'LATEST',
          }), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)

            var shardIterator0 = res.body.ShardIterator

            request(helpers.opts('GetShardIterator', {
              StreamName: stream.StreamName,
              ShardId: 'shardId-1',
              ShardIteratorType: 'LATEST',
            }), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)

              var shardIterator1 = res.body.ShardIterator

              request(helpers.opts('DeleteStream', {StreamName: stream.StreamName}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)

                helpers.waitUntilDeleted(stream.StreamName, function(err, res) {
                  if (err) return done(err)
                  res.body.__type.should.equal('ResourceNotFoundException')

                  assertNotFound({ShardIterator: shardIterator0},
                      'Shard shardId-000000000000 in stream ' + stream.StreamName +
                      ' under account ' + helpers.awsAccountId + ' does not exist', function(err) {
                    if (err) return done(err)

                    assertNotFound({ShardIterator: shardIterator1},
                        'Shard shardId-000000000001 in stream ' + stream.StreamName +
                        ' under account ' + helpers.awsAccountId + ' does not exist', function(err) {
                      if (err) return done(err)

                      stream.ShardCount = 1
                      request(helpers.opts('CreateStream', stream), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)

                        helpers.waitUntilActive(stream.StreamName, function(err, res) {
                          if (err) return done(err)
                          res.statusCode.should.equal(200)

                          assertNotFound({ShardIterator: shardIterator1},
                              'Shard shardId-000000000001 in stream ' + stream.StreamName +
                              ' under account ' + helpers.awsAccountId + ' does not exist', function(err) {
                            if (err) return done(err)

                            request(opts({ShardIterator: shardIterator0}), function(err, res) {
                              if (err) return done(err)
                              res.statusCode.should.equal(200)

                              res.body.Records.should.eql([])
                              helpers.assertShardIterator(res.body.NextShardIterator, stream.StreamName)

                              done()
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
        })
      })
    })
  })

  describe('functionality', function() {

  })

})

