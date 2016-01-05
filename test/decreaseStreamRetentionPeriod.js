var helpers = require('./helpers')

require('should')

var target = 'DecreaseStreamRetentionPeriod',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    randomName = helpers.randomName,
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target)

describe('decreaseStreamRetentionPeriod', function() {

  describe('serializations', function() {

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
    })

    it('should return SerializationException when RetentionPeriodHours is not an Integer', function(done) {
      assertType('RetentionPeriodHours', 'Integer', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no StreamName', function(done) {
      assertValidation({}, [
        'Value null at \'retentionPeriodHours\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', RetentionPeriodHours: -1}, [
        'Value \'-1\' at \'retentionPeriodHours\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, RetentionPeriodHours: 24}, [
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128',
      ], done)
    })

    it('should return InvalidArgumentException for retention period less than 24', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, RetentionPeriodHours: 23},
        'Minimum allowed retention period is 24 hours. ' +
        'Requested retention period (23 hours) is too short.', done)
    })

    it('should return InvalidArgumentException for retention period greater than 168', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, RetentionPeriodHours: 169},
        'Maximum allowed retention period is 168 hours. ' +
        'Requested retention period (169 hours) is too long.', done)
    })

    it('should return InvalidArgumentException for retention period greater than current', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, RetentionPeriodHours: 25},
        'Requested retention period (25 hours) for stream ' + helpers.testStream +
        ' can not be longer than existing retention period (24 hours).' +
        ' Use IncreaseRetentionPeriod API.', done)
    })

    it('should return ResourceNotFoundException if stream does not exist', function(done) {
      var name1 = randomName()
      assertNotFound({StreamName: name1, RetentionPeriodHours: 25},
        'Stream ' + name1 + ' under account ' + helpers.awsAccountId + ' not found.', done)
    })
  })

  describe('functionality', function() {

    it('should decrease stream retention period', function(done) {
      this.timeout(100000)
      request(helpers.opts('IncreaseStreamRetentionPeriod', {
        StreamName: helpers.testStream,
        RetentionPeriodHours: 25,
      }), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        helpers.waitUntilActive(helpers.testStream, function(err, res) {
          if (err) return done(err)

          res.body.StreamDescription.RetentionPeriodHours.should.eql(25)

          request(opts({
            StreamName: helpers.testStream,
            RetentionPeriodHours: 24,
          }), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)

            helpers.waitUntilActive(helpers.testStream, function(err, res) {
              if (err) return done(err)

              res.body.StreamDescription.RetentionPeriodHours.should.eql(24)

              done()
            })
          })
        })
      })
    })

  })

})

