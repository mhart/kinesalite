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

  })

  describe('functionality', function() {

  })

})

