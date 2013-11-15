exports.types = {
  ShardCount: {
    type: 'Long',
    required: true,
    greaterThanOrEqual: 1,
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


