exports.types = {
  Limit: {
    type: 'Integer',
    greaterThanOrEqual: 1,
    lessThanOrEqual: 10000,
  },
  ExclusiveStartStreamName: {
    type: 'String',
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 128,
  },
}

exports.custom = function(data) {
}

