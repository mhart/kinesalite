exports.types = {
  RetentionPeriodHours: {
    type: 'Integer',
    notNull: true,
    greaterThanOrEqual: 1,
    lessThanOrEqual: 10000,
  },
  StreamName: {
    type: 'String',
    notNull: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 128,
  },
}
