var should = require('should'),
    helpers = require('./helpers')

var target = 'ListStreams',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target)

describe('listStreams', function() {

  describe('serializations', function() {

    it('should return SerializationException when Limit is not an Integer', function(done) {
      assertType('Limit', 'Integer', done)
    })

    it('should return SerializationException when ExclusiveStartStreamName is not a String', function(done) {
      assertType('ExclusiveStartStreamName', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for empty ExclusiveStartStreamName', function(done) {
      assertValidation({ExclusiveStartStreamName: '', Limit: 0},
        '3 validation errors detected: ' +
        'Value \'0\' at \'limit\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1; ' +
        'Value \'\' at \'exclusiveStartStreamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'exclusiveStartStreamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for long ExclusiveStartStreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({ExclusiveStartStreamName: name, Limit: 1000000},
        '2 validation errors detected: ' +
        'Value \'1000000\' at \'limit\' failed to satisfy constraint: ' +
        'Member must have value less than or equal to 10000; ' +
        'Value \'' + name + '\' at \'exclusiveStartStreamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128', done)
    })

  })

})

