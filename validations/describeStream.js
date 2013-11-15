exports.types = {
  Limit: {
    type: 'Integer',
    greaterThanOrEqual: 1,
    lessThanOrEqual: 100,
  },
  ExclusiveStartShardId: {
    type: 'String',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 128,
  },
  StreamName: {
    type: 'String',
    required: true,
    streamName: true,
    regex: '[a-zA-Z0-9_.-]+',
  },
}

exports.custom = function(data) {
}



