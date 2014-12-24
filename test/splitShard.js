var should = require('should'),
    helpers = require('./helpers')

var target = 'SplitShard',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target)

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
      assertValidation({},
        '3 validation errors detected: ' +
        'Value null at \'newStartingHashKey\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'shardToSplit\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', NewStartingHashKey: '', ShardToSplit: ''},
        '5 validation errors detected: ' +
        'Value \'\' at \'newStartingHashKey\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: 0|([1-9]\\d{0,38}); ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'shardToSplit\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'shardToSplit\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, NewStartingHashKey: '0', ShardToSplit: name},
        '2 validation errors detected: ' +
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128; ' +
        'Value \'' + name + '\' at \'shardToSplit\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128', done)
    })

    it('should return ResourceNotFoundException if stream does not exist', function(done) {
      var name1 = helpers.randomString(), name2 = helpers.randomString(), name3 = helpers.randomString()
      assertNotFound({StreamName: name1, NewStartingHashKey: name2, ShardToSplit: name3}, [
        'Could not find shard ' + name2 + ' in stream ' + name1 + ' under account ' + helpers.awsAccountId + '.',
        'Could not find shard ' + name3 + ' in stream ' + name1 + ' under account ' + helpers.awsAccountId + '.',
        'Stream ' + name1 + ' under account ' + helpers.awsAccountId + ' not found.',
      ], done)
    })

  })

  describe('functionality', function() {

  })

})

