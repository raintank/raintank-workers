'use strict';

var config = require('../config').config;
var tv4 = require('tv4');
var elasticsearch = require('elasticsearch');
var util = require("util");
var uuid = require('node-uuid');

var client;

function getClient() {
	if (!client) {
		client = new elasticsearch.Client(config.elasticsearch);
	}
	return client;
}

var schema = {
	title: "Event Definition",
	type: "object",
	properties: {
		id: {type: "string"},
		event_type: {type: "string"},
	    org_id: {type: "integer"},
	    severity: {type: "string", enum: ["INFO", "WARN", "ERROR", "OK"]},
	    source: {type: "string"},
	    timestamp: {type: "integer"},
	    message: {type: "string"}
	},
	required: ["event_type", "org_id", "severity", "source", "timestamp", "message"]
};

exports.schema = schema;

var Model = function(def) {
	this.body = def;
};
module.exports.Model = Model;
module.exports.getClient = getClient;

exports.Model.prototype.save = function(callback) {
	console.log('saving Event object.');
	var self = this;
	if (!self.body.id) {
		self.body.id = uuid.v4();
	}
	if (!self.body.timestamp) {
		self.body.timestamp = new Date().getTime();
	}

	self.validate(function(isValid, validationError) {
		if (! isValid) {
			console.log('event not valid.');
			console.log(validationError);
			return callback(validationError);
		}
		console.log('event is valid.');
		//store in elastic search.

		getClient().create({
			index: "events",
			type: self.body.event_type,
			id: self.body.id,
			body: self.body
		}, function(error, response) {
			if (error) {
				return callback(error);
			}
			return callback(null, self);
		});
	});
};

exports.Model.prototype.validate = function(callback) {
	console.log('validating Event Object.');
	var result = tv4.validateResult(this.body, schema);
	if (result.valid) {
		return callback(true);
	}
	return callback(false, result.error);
}