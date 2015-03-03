'use strict';
//Load the config file.
var config = require('./config/config.json');

/*-------------------------------------------------------*/
function parseEnv(name, value, cnf) {
  if (name in cnf) {
    cnf[name] = value;
    return;
  }
  var parts = name.split('_');
  var pos = 0
  var found = false
  while (!found && pos < parts.length) {
    pos = pos + 1;
    var group = parts.slice(0,pos).join('_');
    if (group in cnf){
      found = true;
      parseEnv(parts.slice(pos).join('_'), value, cnf[group]);
    }
  }
  if (!found) {
    console.log("%s not found in config", name);
  }
}

// overwrite with Environment variables
for (var key in process.env) {
  if (key.indexOf('RAINTANK_') == 0) {
    var name = key.slice(9);
    parseEnv(name, process.env[key], config);
  }
}
exports.config = config;
