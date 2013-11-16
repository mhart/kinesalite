exports.types = {
  NewStartingHashKey: {
    type: 'String',
    required: true,
    lengthGreaterThanOrEqual: 1,
  },
  ShardToSplit: {
    type: 'String',
    required: true,
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

