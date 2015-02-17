'use strict';

var config = require('../config').config;
var tv4 = require('tv4');
var elasticsearch = require('elasticsearch');
var util = require("util");
var redis = require("redis"),
redisClient = redis.createClient(config.redis.port, config.redis.host);
redisClient.on("error", function (err) {
    console.log("redisClient Error " + err);
});

var client;

function getClient() {
	if (!client) {
		client = new elasticsearch.Client(config.elasticSearch);
	}
	return client;
}

var schema = {
	title: "Metric Definition",
	type: "object",
	properties: {
		id: {type: "string"},
		name: {type: "string"},
	    account: {type: "integer"},
	    location: {type: "string"},
	    metric: {type: "string"},
	    target_type: {type: "string", enum: ['derive', 'gauge']},
	    unit: {type: 'string'},
	    interval: {type: 'integer', minimum: 10},
	    lastUpdate: {type: "integer"},
	    site: {type: "integer"},
	    monitor: {type: "integer"},
	    thresholds: {
	        warnMin: {type: "integer"},
	        warnMax: {type: "integer"},
	        criticalMin: {type: "integer"},
	        criticalMax: {type: "integer"}
	    },
	    keepAlives: {type: "boolean", default: false},
	    state: {type: "integer", min: 0, max: 2, default: 0},
	},
	required: ["name", "account", "target_type", "interval", "metric", "unit"]
};

exports.schema = schema;

var Model = function(def) {
	for (var prop in def) {
		this[prop] = def[prop];
	}
};
module.exports.Model = Model;
module.exports.getClient = getClient;

Model.prototype.save = function(callback) {
	console.log('saving Defintion object.');
	if (this._commited) {
		return this.update(callback);
	}
	var self = this;
	if (!self.id) {
		self.id = util.format("%s.%s", self.account, self.name);
	}
	if (!self.lastUpdate) {
		self.lastUpdate = new Date().getTime();
	}

	self.validate(function(isValid, validationError) {
		if (! isValid) {
			console.log('definition not valid.');
			console.log(self);
			console.log(validationError);
			return callback(validationError);
		}
		console.log('definition is valid.');
		//separate out the Id form the body.
		var body = {};
		for (var key in schema.properties) {
			if ((key != 'id') && (key in self)) {
				body[key] = self[key];
			}
		}
		//store in elastic search.
		getClient().create({
			index: "definitions",
			type: "metric",
			id: self.id,
			body: body
		}, function(error, response) {
			if (error) {
				return callback(error);
			}
			console.log("def saved to elasticsearch.")
			console.log(self);
			return callback(null, self);
		});
	});
};

Model.prototype.update = function(callback) {
	console.log('updating Defintion object.');

	var self = this;

	self.validate(function(isValid, validationError) {
		if (! isValid) {
			console.log('definition not valid.');
			console.log(self);
			console.log(validationError);
			return callback(validationError);
		}
		//separate out the Id form the body.
		var body = { doc: {}};
		for (var key in schema.properties) {
			if ((key != 'id') && (key in self)) {
				body.doc[key] = self[key];
			}
		}

		console.log('definition is valid.');
		//store in elastic search.
		getClient().update({
			index: "definitions",
			type: "metric",
			id: self.id,
			body: body
		}, function(error, response) {
			if (error) {
				return callback(error);
			}
			return callback(null, self);
		});
	});
}

Model.prototype.validate = function(callback) {
	console.log('validating metricDefition Object.');
	var result = tv4.validateResult(this, schema);
	if (result.valid) {
		return callback(true);
	}
	return callback(false, result.error);
}

//class method
exports.find = function(filter, size, callback) {
	console.log('performing search for metricDefinition Objects.');

	getClient().search({
		index: 'definitions',
		type: 'metric',
		body: {
			query: filter,
			size: size,
			sort: [
            	{ 
                	name : {order: "desc"}
            	}
        	]
		}
	}, function(err, resp) {
		if (err) {
			return callback(err);
		}
		var response = [];
		if (resp.hits.total > 0) {
			resp.hits.hits.forEach(function(hit) {
				var payload = hit._source;
				payload.id = hit._id;
				response.push(payload);
			});
		}
		var objects = [];
		response.forEach(function(e) {
			e._commited = true; //flag that this came from elasticsearch.
			objects.push(new Model(e));
		});
		return callback(null, objects)
	});
}

//class method
exports.get = function(id, callback) {
	console.log('retrieving metricDef Object by ID.', id);
	redisClient.get(id, function(err, result) {
		if (err) {
			console.log("Error from redisClient.", err)
			callback(err, null);
		}
		if (result) {
			return callback(null, new Model(JSON.parse(result)));
		}
		//else get the result from elasticsearch.
		getClient().get({
			index: 'definitions',
			type: 'metric',
			id: id
		}, function(err, response) {
			if (err) {
				console.log("error with get request to elasticsearch.", err);
				if (err.message === "IndexMissingException[[definitions] missing]") {
					return callback();
				}
				if (err.message === 'Not Found' ) {
					return callback();
				}
				return callback(err);
			}
			var obj = null;
			if (response.found) {
				response._source.id = response._id;
				response._source._commited = true;  //flag that this came from elasticsearch.
				redisClient.setex(id, 300, JSON.stringify(response._source))
				obj = new Model(response._source);
			}
			callback(null, obj);
		});
	});
}
