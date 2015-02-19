'use strict';
var config = require('./config').config;
var util = require('util');
var queue = require('raintank-queue');
var cluster = require('cluster');
var EventDefinitions = require("./lib/eventDefinitions");

var numCPUs = config.numCPUs;
var client;
function init() {
    console.log("initializing");
    var eventConsumer = new queue.Consumer({
        url: config.queue.url,
        exchangeName: "grafana_events",  //this should match the name of the exchange the producer is using.
        exchangeType: "topic", // this should match the exchangeType the producer is using.
        queueName: '', //leave blank for an auto generated name. recommended when creating an exclusive queue.
        exclusive: true, //make the queue exclusive.
        durable: false,
        autoDelete: true,
        queuePattern: 'EVENT.#', //match all EVENTS
        retryCount: -1, // keep trying to connect forever.
        handler: processEvent
    });
    eventConsumer.on('error', function(err) {
        console.log("eventConsumer emitted fatal error.")
        console.log(err);
        process.exit(1);
    });
}

function processEvent(message) {
    var event = JSON.parse(message.content.toString());
    var obj = new EventDefinitions.Model(event);
    obj.save(function(err) {
        if (err) {
            console.log('failed to save serviceEvent.');
            console.log(err);
            return;
        }
        console.log(obj);
        console.log('Event saved.');
    });
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