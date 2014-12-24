var should = require('should'),
    helpers = require('./helpers')

var target = 'MergeShards',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target)

describe('mergeShards', function() {

  describe('serializations', function() {

    it('should return SerializationException when AdjacentShardToMerge is not an String', function(done) {
      assertType('AdjacentShardToMerge', 'String', done)
    })

    it('should return SerializationException when ShardToMerge is not a String', function(done) {
      assertType('ShardToMerge', 'String', done)
    })

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no StreamName', function(done) {
      assertValidation({},
        '3 validation errors detected: ' +
        'Value null at \'shardToMerge\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'adjacentShardToMerge\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', AdjacentShardToMerge: '', ShardToMerge: ''},
        '6 validation errors detected: ' +
        'Value \'\' at \'shardToMerge\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'shardToMerge\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'adjacentShardToMerge\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'adjacentShardToMerge\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, AdjacentShardToMerge: name, ShardToMerge: name},
        '3 validation errors detected: ' +
        'Value \'' + name + '\' at \'shardToMerge\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128; ' +
        'Value \'' + name + '\' at \'adjacentShardToMerge\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128; ' +
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128', done)
    })

    it('should return ResourceNotFoundException if stream does not exist', function(done) {
      var name1 = helpers.randomString(), name2 = helpers.randomString(), name3 = helpers.randomString()
      assertNotFound({StreamName: name1, AdjacentShardToMerge: name2, ShardToMerge: name3}, [
        'Could not find shard ' + name3 + ' in stream ' + name1 + ' under account ' + helpers.awsAccountId + '.',
        'Could not find shard ' + name2 + ' in stream ' + name1 + ' under account ' + helpers.awsAccountId + '.',
        'Stream ' + name1 + ' under account ' + helpers.awsAccountId + ' not found.',
      ], done)
    })

  })

  describe('functionality', function() {

  })

})

