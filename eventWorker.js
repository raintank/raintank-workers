'use strict';

var schema = require('raintank-core/schema');
var util = require('util');
var config = require('raintank-core/config');
var kafka = require('kafka-node');
var HighLevelConsumer = kafka.HighLevelConsumer;

var running = false;
var client;
function init() {

    client = new kafka.Client(config.kafka.connectionString, 'eventWorker', {sessionTimeout: 1500});
    running = true;
    var consumer = new HighLevelConsumer(
        client,
        [
            { topic: 'serviceEvents'},
            { topic: 'metricEvents'}
        ],
        {
            groupId: "EventWorker",
            autoCommitIntervalMs: 1000,
            // The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
            fetchMaxBytes: 1024 * 10, 
        }
    );
    consumer.on('error', function(err) {
        console.log('consumer emiited error.');
        console.log(err);
        if (running) {
            console.log('closing client');
            client.close();
            running = false;
            setTimeout(function() {
                console.log("Restarting worker");
                init();
            },2500);
        } else {
            console.log("client already closed.")
        }
    });

    consumer.on('message', function (message) {

        if (message.topic == "serviceEvents") {
            serviceEvent(message);
        } else if (message.topic == "metricEvents") {
            metricEvent(message);
        }
    });
}

function serviceEvent(message) {
    var serviceEvent = JSON.parse(message.value);
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

function metricEvent(message) {
    var metricEvent = JSON.parse(message.value);
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
    client.close();
    process.exit();
});

init();