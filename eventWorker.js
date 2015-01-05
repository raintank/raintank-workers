'use strict';
var config = require('./config').config;
var schema = require('raintank-core/schema');
var util = require('util');
var queue = require('raintank-queue');
var consumer = new queue.Consumer({
    mgmtUrl: config.queue.mgmtUrl,
    consumerSocketAddr: config.queue.consumerSocketAddr
});
var cluster = require('cluster');

var numCPUs = config.numCPUs;

var running = false;
var client;
function init() {
    console.log("initializing");
    consumer.on('connect', function() {
        consumer.join('serviceEvents', 'eventWorker');
        consumer.join('metricEvents', 'eventWorker');
    });

    consumer.on('message', function (topic, partition, message) {
        if (topic == "serviceEvents") {
            serviceEvent(message);
        } else if (topic == "metricEvents") {
            metricEvent(message);
        }
    });
}

function serviceEvent(serviceEvent) {
    console.log('processing serviceEvent.');
    var obj = new schema.serviceEvents.model(serviceEvent);
    obj.save(function(err) {
        if (err) {
            console.log('failed to save serviceEvent.');
            console.log(err);
            return;
        }
        console.log(obj);
        console.log('serviceEvent saved.');
    });
    //TODO: handle Actions via ElasticSearch Percolate.
};

function metricEvent(metricEvent) {
    if (metricEvent.type == 'keepAlive') {
        //ignore keepalives that are older then 2minute2.
        if (metricEvent.timestamp > (new Date().getTime() - 120000)) {
            console.log('keepAlive recieved for: %s.%s', metricEvent.account, metricEvent.metric);
        }
        return;
    }
    console.log('processing metricEvent.');
    var obj = new schema.metricEvents.model(metricEvent);
    obj.save(function(err) {
        if (err) {
            console.log('failed to save metricEvent.');
            console.log(err);
            return;
        }
        console.log(obj);
        console.log('metricEvent saved.');
    });
    //TODO: handle Actions via ElasticSearch Percolate.
};


process.on( "SIGINT", function() {
    console.log('CLOSING [SIGINT]');
    process.exit();
});

if (cluster.isMaster) {
    // Fork workers.
    for (var i = 0; i < numCPUs; i++) {
        cluster.fork();
    }
    cluster.on('exit', function(worker, code, signal) {
        console.log('worker ' + worker.process.pid + ' died');
        cluster.fork();
    });
} else {
    init();
}