exports.types = {
  ScalingType: {
    type: 'String',
    notNull: true,
    enum: ['UNIFORM_SCALING'],
  },
  StreamName: {
    type: 'String',
    notNull: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 128,
  },
  TargetShardCount: {
    type: 'Integer',
    notNull: true,
    greaterThanOrEqual: 1,
    lessThanOrEqual: 100000,
  },
}
