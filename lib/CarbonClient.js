var poolModule = require('generic-pool');
var url        = require('url');
var net        = require('net');

module.exports = CarbonClient;

function CarbonClient(properties) {
  properties = properties || {};
  var self = this;
  this._dsn    = properties.dsn;
  var dsn  = url.parse(this._dsn);
  this._pool   = poolModule.Pool({
      name     : 'carbon-cache',
      create   : function(callback) {
          var c = net.createConnection(parseInt(dsn.port, 10) || 2003, dsn.hostname)
          c.on('error', function(err) {
            console.log('destroying carbon-cache client.');
            self._pool.destroy(c);
          });

          // parameter order: err, resource
          // new in 1.0.6
          callback(null, c);
      },
      destroy  : function(client) { console.log('destroying connection to carbon-cache'); client.end(); },
      max      : 10,
      // specifies how long a resource can stay idle in pool before being removed
      idleTimeoutMillis : 30000,
      reapIntervalMillis : 10000,
       // if true, logs via console.log - can also be a function
      log : false 
  });
}

CarbonClient.prototype.write = function(metrics, cb) {
  var lines = '';
  metrics.forEach(function(line) {
    lines += line + '\n';
  });
  var self = this;
  this._pool.acquire(function(err, client) {
      if (err) {
          // handle error - this is generally the err from your
          // factory.create function
          console.log("error: could not aquire connection to carbon-cache from pool.")
          cb(err);
      }
      else {
          client.write(lines, 'utf-8', function(err) {
              if (err) {
                console.log('Error while writing to carbon-cache.');
                // our onError handler, will ensure this connection gets removed from the pool.
                return setTimeout(function() {
                  console.log("retrying write to carbon.");
                  self.write(metrics, cb);
                }, 1000);
              } else {
                self._pool.release(client);
              }
              cb(err);
          });
      }
  });
};
