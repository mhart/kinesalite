exports.types = {
  ShardId: {
    type: 'String',
    required: true,
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 128,
  },
  ShardIteratorType: {
    type: 'String',
    required: true,
    enum: ['AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER', 'TRIM_HORIZON', 'LATEST'],
  },
  StartingSequenceNumber: {
    type: 'String',
    lengthGreaterThanOrEqual: 1,
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

