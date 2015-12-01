var helpers = require('./helpers')

require('should');

var target = 'IncreaseStreamRetentionPeriod',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    randomName = helpers.randomName,
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target)

describe('increaseStreamRetentionPeriod', function() {

  describe('validations', function() {

    it('should return ValidationException for no StreamName', function(done) {
      assertValidation({},
        '2 validation errors detected: ' +
        'Value null at \'retentionPeriodHours\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', RetentionPeriodHours: -1},
        '3 validation errors detected: ' +
        'Value \'-1\' at \'retentionPeriodHours\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1; Value \'\' at \'streamName\' ' +
        'failed to satisfy constraint: Member must satisfy regular expression pattern: ' +
        '[a-zA-Z0-9_.-]+; Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, RetentionPeriodHours: 24},
        '1 validation error detected: ' +
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128', done)
    })

    it('should return InvalidArgumentException for retention period less than 24', function(done) {
      var hours = 23
      assertInvalidArgument({StreamName: helpers.testStream, RetentionPeriodHours: hours},
        'Minimum allowed retention period is 24 hours. ' +
        'Requested retention period (' + hours + ' hours) is too short.', done)
    })

    it('should return InvalidArgumentException for retention period greater than 168', function(done) {
      var hours = 169
      assertInvalidArgument({StreamName: helpers.testStream, RetentionPeriodHours: hours},
        'Maximum allowed retention period is 168 hours. ' +
        'Requested retention period (' + hours + ' hours) is too long.', done)
    })

    it('should return InvalidArgumentException for retention period less than current', function(done) {
      var hours = 24

      request(opts({
        StreamName: helpers.testStream,
        RetentionPeriodHours: 25,
      }), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        assertInvalidArgument({StreamName: helpers.testStream, RetentionPeriodHours: hours},
          'Requested retention period (' + hours + ' hours) for stream ' + helpers.testStream +
          ' can not be shorter than existing retention period (25 hours).' +
          ' Use DecreaseRetentionPeriod API.', done)
      })
    })

    it('should return ResourceNotFoundException if stream does not exist', function(done) {
      var name1 = randomName()
      assertNotFound({StreamName: name1, RetentionPeriodHours: 25 },
        'Stream ' + name1 + ' under account ' + helpers.awsAccountId + ' not found.', done)
    })
  })

  describe('functionality', function() {

    it('should increase stream retention period', function(done) {
      var hours = 25
      request(opts({
        StreamName: helpers.testStream,
        RetentionPeriodHours: hours,
      }), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        request(helpers.opts('DescribeStream', {
          StreamName: helpers.testStream,
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          res.body.StreamDescription.RetentionPeriodHours.should.eql(hours)

          done();
        })
      })
    })

  })

})

