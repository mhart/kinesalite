exports.types = {
  ShardId: {
    type: 'String',
    notNull: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 128,
  },
  ShardIteratorType: {
    type: 'String',
    notNull: true,
    enum: ['AFTER_SEQUENCE_NUMBER', 'LATEST', 'AT_SEQUENCE_NUMBER', 'TRIM_HORIZON'],
  },
  StartingSequenceNumber: {
    type: 'String',
    regex: '0|([1-9]\\d{0,128})',
  },
  StreamName: {
    type: 'String',
    notNull: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 128,
  },
}
