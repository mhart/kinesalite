var helpers = require('./helpers')

var target = 'DescribeStreamSummary',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('describeStreamSummary', function() {

  describe('serializations', function() {

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no StreamName', function(done) {
      assertValidation({}, [
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: ''}, [
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name}, [
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128',
      ], done)
    })

    it('should return ResourceNotFoundException if stream does not exist', function(done) {
      var name = randomName()
      assertNotFound({StreamName: name}, 'Stream ' + name + ' under account ' +
        helpers.awsAccountId + ' not found.', done)
    })

  })

  describe('functionality', function() {

    it('should return stream description summary', function(done) {
      request(opts({StreamName: helpers.testStream}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        var createTime = Date.now() / 1000

        res.body.StreamDescriptionSummary.StreamCreationTimestamp.should.be.above(createTime - 10)
        res.body.StreamDescriptionSummary.StreamCreationTimestamp.should.be.below(createTime + 10)
        delete res.body.StreamDescriptionSummary.StreamCreationTimestamp

        res.body.should.eql({
          StreamDescriptionSummary: {
            StreamStatus: 'ACTIVE',
            StreamName: helpers.testStream,
            StreamARN: 'arn:aws:kinesis:' + helpers.awsRegion + ':' + helpers.awsAccountId +
              ':stream/' + helpers.testStream,
            RetentionPeriodHours: 24,
            EncryptionType: 'NONE',
            EnhancedMonitoring: [{ShardLevelMetrics: []}],
            OpenShardCount: 3,
          },
        })

        done()
      })
    })

  })

})

