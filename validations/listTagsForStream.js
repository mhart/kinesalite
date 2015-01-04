exports.types = {
  Limit: {
    type: 'Integer',
    greaterThanOrEqual: 1,
    lessThanOrEqual: 10,
  },
  ExclusiveStartTagKey: {
    type: 'String',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 128,
  },
  StreamName: {
    type: 'String',
    notNull: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 128,
  },
}

exports.custom = function(data) {
}

