exports.types = {
  Limit: {
    type: 'Integer',
    greaterThanOrEqual: 1,
    lessThanOrEqual: 10000,
  },
  ShardIterator: {
    type: 'String',
    notNull: true,
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 512,
  },
}

exports.custom = function(data) {
}

