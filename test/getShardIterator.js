var should = require('should'),
    helpers = require('./helpers')

var target = 'GetShardIterator',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target)

describe('getShardIterator', function() {

  describe('serializations', function() {

    it('should return SerializationException when ShardId is not an String', function(done) {
      assertType('ShardId', 'String', done)
    })

    it('should return SerializationException when ShardIteratorType is not a String', function(done) {
      assertType('ShardIteratorType', 'String', done)
    })

    it('should return SerializationException when StartingSequenceNumber is not a String', function(done) {
      assertType('StartingSequenceNumber', 'String', done)
    })

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no StreamName', function(done) {
      assertValidation({},
        '3 validation errors detected: ' +
        'Value null at \'shardId\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'shardIteratorType\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', ShardIteratorType: '', ShardId: '', StartingSequenceNumber: ''},
        '6 validation errors detected: ' +
        'Value \'\' at \'shardId\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'shardId\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'shardIteratorType\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [AFTER_SEQUENCE_NUMBER, LATEST, AT_SEQUENCE_NUMBER, TRIM_HORIZON]; ' +
        'Value \'\' at \'startingSequenceNumber\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: 0|([1-9]\\d{0,128}); ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, ShardId: name, ShardIteratorType: 'LATEST'},
        '2 validation errors detected: ' +
        'Value \'' + name + '\' at \'shardId\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128; ' +
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128', done)
    })

    it('should return ResourceNotFoundException if stream does not exist', function(done) {
      var name1 = helpers.randomString(), name2 = helpers.randomString()
      assertNotFound({StreamName: name1, ShardId: name2, StartingSequenceNumber: '0', ShardIteratorType: 'LATEST'},
        'Shard ' + name2 + ' in stream ' + name1 + ' under account ' + helpers.awsAccountId + ' does not exist', done)
    })

  })

  describe('functionality', function() {

  })

})

