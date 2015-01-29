var CarbonClient = require('./CarbonClient');
var config = require('../config').config;
var util = require('util');

var carbon = new CarbonClient({dsn: util.format('plaintext://%s:%s/', config.carbon.host, config.carbon.port)});

module.exports = carbon;