'use strict;'
var config = require('./config').config;
var util = require('util');
var schema = require('raintank-core/schema');
var queue = require('raintank-queue');
var consumer = new queue.Consumer({
    mgmtUrl: config.queue.mgmtUrl
});
var producer = queue.Publisher;
producer.init({
    publisherSocketAddr: config.queue.publisherSocketAddr,
    partitions: config.queue.partitions,
});

function dispatch() {
    var now = new Date().getTime();
    // schedule next run.
    next_run(now);
    // get position from current timestamp.
    var second = (now/1000) % 3600;
    second = Math.floor(second);
    var filter = {
        position: second
    }

    // find all taskSchedules that should be run now.
    schema.taskSchedules.model.find(filter).lean().exec(function(err, taskSchedules) {
        if (err){
            console.log('ERROR getting collectorTaskSchedules');
            return;
        }
        if (taskSchedules.length < 1) {
            return;
        }
        var messages = [];
        taskSchedules.forEach(function(ts) {
            ts.timestamp = now;
            console.log('sending task to queue.');
            messages.push(ts);
        });
        console.log("sending messages to queue.: %j", messages);
        producer.send('tasks', messages);
    });
}

var isLeader = false;
var timer;

function init() {
    console.log('waiting for dispatcher_lock');
    
    consumer.on('connect', function() {
        consumer.join('lock', 'dispatcher');
    });
    consumer.on('ready', function(data) {
        //who ever has the '0' partition is the leader.
        if (data.partitions.indexOf(0) > -1) {
            if (!isLeader) {
                console.log('taking over as leader.');
                isLeader = true;
                next_run(new Date().getTime());
            }
        } else {
            isLeader = false;
            clearTimeout(timer);
        }
    });
    consumer.on('disconnect', function() {
        isLeader = false;
        clearTimeout(timer);
    });

}

function next_run(now) {
    // schedule next run.
    var drift = (now % 1000);
    if (drift > 500) {
        //TODO: send drift to statsD
        console.log('WARNING: drift is ' + drift);
    }

    if (isLeader) {
        timer = setTimeout(dispatch, (1000 - drift));
    }
}
init();

process.on( "SIGINT", function() {
    console.log('CLOSING [SIGINT]');
    process.exit();
});
