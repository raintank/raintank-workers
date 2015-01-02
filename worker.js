'use strict';

var config = require('./config').config;
var schema = require('raintank-core/schema');
var util = require('util');
var serviceTypes = require('raintank-core/serviceTypes');
var continousQueryTypes = require('raintank-core/queryTypes');
var queue = require('raintank-queue');
var consumer = new queue.Consumer({
    mgmtUrl: config.queue.mgmtUrl
});

var running = false;
var client;

function init() {
    consumer.on('connect', function() {
        consumer.join('tasks', 'taskWorker');
    });

    consumer.on('message', function (topic, partition, message) {
        var ts = message
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
    process.exit();
});

init();