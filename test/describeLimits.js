var helpers = require('./helpers')

var target = 'DescribeLimits',
    request = helpers.request,
    opts = helpers.opts.bind(null, target)

describe('describeLimits', function() {

  describe('functionality', function() {

    // note there is already a stream due to helpers.before

    it('should return current account limits', function(done) {
      request(opts({}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        Object.keys(res.body).sort().should.eql(['OpenShardCount', 'ShardLimit'])
        res.body.OpenShardCount.should.equal(3)
        res.body.ShardLimit.should.equal(helpers.shardLimit)
        done()
      })
    })
  })
})
