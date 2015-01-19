var should = require('should'),
    helpers = require('./helpers')

var target = 'PutRecords',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target)

describe('putRecords', function() {

  describe('serializations', function() {

    it('should return SerializationException when Records is not a list', function(done) {
      assertType('Records', 'List', done)
    })

    it('should return SerializationException when Records.0 is not a struct', function(done) {
      assertType('Records.0', 'Structure', done)
    })

    it('should return SerializationException when Records.0.Data is not a Blob', function(done) {
      assertType('Records.0.Data', 'Blob', done)
    })

    it('should return SerializationException when Records.0.ExplicitHashKey is not a String', function(done) {
      assertType('Records.0.ExplicitHashKey', 'String', done)
    })

    it('should return SerializationException when Records.0.PartitionKey is not a String', function(done) {
      assertType('Records.0.PartitionKey', 'String', done)
    })

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no StreamName', function(done) {
      assertValidation({},
        '2 validation errors detected: ' +
        'Value null at \'records\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', Records: []},
        '3 validation errors detected: ' +
        'Value \'[]\' at \'records\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, Records: [{PartitionKey: 'a', Data: ''}]},
        '1 validation error detected: ' +
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128', done)
    })

    it('should return ResourceNotFoundException if stream does not exist', function(done) {
      var name1 = helpers.randomString()
      assertNotFound({StreamName: name1, Records: [{PartitionKey: 'a', Data: ''}]},
        'Stream ' + name1 + ' under account ' + helpers.awsAccountId + ' not found.', done)
    })

  })

  describe('functionality', function() {

  })

})

// In PutRecords, a block of SequenceNumbers is reserved for each shard, eg Shard 0: 1-100, 1: 101-200, 2: 201-300
