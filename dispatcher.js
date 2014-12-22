'use strict;'
var util = require('util');
var schema = require('raintank-core/schema');
var config = require('raintank-core/config');
var zookeeper = require('node-zookeeper-client');
var producer = require('raintank-core/lib/kafka').producer;

function dispatch() {
    var now = new Date().getTime();
    
    // schedule next run.
    var drift = (now % 1000);
    if (isleader) {
        setTimeout(dispatch, (1000 - drift));
    } else {
        return init();
    }

    if (drift > 500) {
        //TODO: send drift to statsD
        console.log('WARNING: drift is ' + drift);
    }
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
            messages.push(JSON.stringify(ts));
        });
        console.log("sending messages to queue.: %s", messages);
        producer.send([{topic: 'tasks', messages: messages}], function(err, data) {
            if (err) {
                console.log('failed to send tasks to task queue.');
                console.log(err);
            }
        });
    });
}

var zclient;
var isleader = false;

function init() {
    console.log('waiting for dispatcher_lock');
    zclient = zookeeper.createClient(config.zookeeper.connectionString, {sessionTimeout: 1000});
    zclient.once('connected', function() {
        console.log('connected to zookeeper.');
        zclient.mkdirp('/worker_leader', function(err) {
            if (err) {
                console.log('failed to create worker_leader znode');
                console.log(err);
                return;
            }
            var path = '/worker_leader/n_';
            zclient.create(
                path,
                null,
                zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL,
                function (error, data) {
                    if (error) {
                        console.log('Failed to create node: %s due to: %s.', path, error);
                        setTimeout(function(){
                            init();
                        }, 1000);
                        return;
                    } else {
                        console.log(data);
                        var current_id = data.split('/').pop();
                        console.log('Node: %s is successfully created.', data);
                        console.log('current_id: %s', current_id);
                        var until_leader = function() {
                            zclient.getChildren('/worker_leader', function(err, children, stats ) {
                                if (error) {
                                    console.log('failed to get children. %s', err);
                                    return until_leader();
                                }
                                console.log("children are: %j", children);
                                children.sort();
                                console.log(children);
                                var leader = children[0];
                                if (leader == current_id) {
                                    console.log('taking role as leader.');
                                    isleader = true;
                                    run();
                                } else {
                                    console.log('we are not the leader. leader is %s', leader);
                                    zclient.exists('/worker_leader/'+leader, function(event) {
                                        console.log('got evenet: %s', event);
                                        return until_leader();
                                    }, function(err, stat) {
                                        if (err) {
                                            console.log('could not get status of: %s', current_id);
                                            return until_leader();
                                        }
                                        //TODO: handle race condition of the leader being removed
                                        // before zclient.exists is called.  
                                        console.log('waiting for leader to change');
                                    });
                                }
                            });
                        }
                        until_leader();
                    }
                }
            );
        });
    });

    zclient.on('close', function() {
        console.log('connection closed');
        isleader = false;
    });

    zclient.on('error', function(err) {
        console.log(err);
        isleader = false;
    });
    zclient.connect();
}


function run() {
    var now = new Date().getTime();

    //start execution near to the beginning of the second.
    var milliseconds = now % 1000;

    setTimeout(function(){
        var start = new Date();
        console.log('Launching dispatcher: ' + start + ' ' + start.getTime());

        dispatch();
    }, (1000 - milliseconds));
}

init();

process.on( "SIGINT", function() {
    console.log('CLOSING [SIGINT]');
    zclient.close();
    process.exit();
});
