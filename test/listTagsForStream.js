var should = require('should'),
    helpers = require('./helpers')

var target = 'ListTagsForStream',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target)

describe('listTagsForStream', function() {

  describe('serializations', function() {

    it('should return SerializationException when Limit is not an Integer', function(done) {
      assertType('Limit', 'Integer', done)
    })

    it('should return SerializationException when ExclusiveStartTagKey is not a String', function(done) {
      assertType('ExclusiveStartTagKey', 'String', done)
    })

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no StreamName', function(done) {
      assertValidation({},
        '1 validation error detected: ' +
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', ExclusiveStartTagKey: '', Limit: 0},
        '4 validation errors detected: ' +
        'Value \'0\' at \'limit\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1; ' +
        'Value \'\' at \'exclusiveStartTagKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, ExclusiveStartTagKey: name, Limit: 100},
        '3 validation errors detected: ' +
        'Value \'100\' at \'limit\' failed to satisfy constraint: ' +
        'Member must have value less than or equal to 10; ' +
        'Value \'' + name + '\' at \'exclusiveStartTagKey\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128; ' +
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128', done)
    })

    it('should return ResourceNotFoundException if stream does not exist', function(done) {
      var name1 = helpers.randomString()
      assertNotFound({StreamName: name1, ExclusiveStartTagKey: 'a', Limit: 1},
        'Stream ' + name1 + ' under account ' + helpers.awsAccountId + ' not found.', done)
    })

  })

  describe('functionality', function() {

  })

})

