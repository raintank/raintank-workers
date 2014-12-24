'use strict';

var config = require('./config').config;
var schema = require('raintank-core/schema');
var util = require('util');
var serviceTypes = require('raintank-core/serviceTypes');
var continousQueryTypes = require('raintank-core/queryTypes');
var kafka = require('kafka-node');
var HighLevelConsumer = kafka.HighLevelConsumer;

var running = false;
var client;

function init() {

    client = new kafka.Client(config.kafka.connectionString, 'taskWorker', {sessionTimeout: 1500});
    running = true;
    var consumer = new HighLevelConsumer(
        client,
        [
            { topic: 'tasks'}
        ],
        {
            groupId: "taskWorker",
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
        console.log(message);
        var ts = JSON.parse(message.value);
        console.log(ts);
        if (!(ts._id)) {
            console.log("invalid msg content.");
            return;
        }
        if (ts.timestamp < (new Date().getTime() - 60000)) {
            console.log('task is too old. skipping.');
            return;
        }  
        var populate = null;
        var cls = null;
        if (ts.parent.class == 'services') {
            populate = 'serviceType';
            cls = serviceTypes;
        } else if (ts.parent.class == 'continuousQuery') {
            populate = 'continuousQueryType';
            cls = ccontinousQueryTypes;
        }
        schema[ts.parent.class].model.findOne({_id: ts.parent.id}).populate(populate).exec(function(err, task) {
            if (err) {
                console.log(err);
            }
            if (!task) {
                console.log('task not found in DB.');
                return;
            }
            if (task.enabled) {
                var Task = cls[task[populate]._id].factory(task);
                console.log(util.format('disptching %s task: %s', ts.parent.class, task._id));
                Task.run(ts.timestamp);
            } else {
                console.log(ts.parent.class + " task " + ts.parent.id + " is paused.");
            }
        });
    });
}

process.on( "SIGINT", function() {
    console.log('CLOSING [SIGINT]');
    client.close();
    process.exit();
});

init();