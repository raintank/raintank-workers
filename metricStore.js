'use strict';
var config = require('./config').config;
var schema = require('raintank-core/schema');
var util = require('util');
var carbon = require('raintank-core/lib/carbon');
var kafka = require('kafka-node');
var HighLevelConsumer = kafka.HighLevelConsumer;
var producer = require('raintank-core/lib/kafka').producer;
var cluster = require('cluster');

var numCPUs = config.numCPUs;

var running = false;
var client;


var metricDef = {};

function init() {
    console.log("initializing");
    client = new kafka.Client(config.kafka.connectionString, 'metricStore', {sessionTimeout: 1500});
    running = true;
    var consumer = new HighLevelConsumer(
        client,
        [
            { topic: 'metrics'},
            { topic: 'metricDefChange'}
        ],
        {
            groupId: "metricStore",
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
        }
        console.log("exiting");
        process.exit(1);
    });
    consumer.on('message', function (message) {
        if (message.topic == "metrics") {
            processMetric(message);
        } else if (message.topic == "metricDefChange") {
            processMetricDefEvent(message);
        }
    });
    // every 15minutes, delete any stale metricDefs from the cache.
    setInterval(function() {
        var metrics = Object.keys(metricDef);
        metrics.forEach(function(metricName) {
            var metric = metricDef[metricName];
            if (metric.lastUpdate.getTime() < (new Date().getTime() - 900000)) {
                delete metricDef[metricName];
            }
        });
    }, 900000);
}



var buffer = {
    lastFlush: new Date().getTime(),
    metrics: [],
}


function processMetricDefEvent(message) {
    var payload = JSON.parse(message.value);
    if (payload.action == 'update') {
        updateMetricDef(payload.metric);
    } else if (payload.action == 'remove') {
        removeMetricDef(payload.metric);
    }
}

function updateMetricDef(metric) {
    if (!(metric._id in metricDef)) {
        return;
    }
    console.log('updating metricDef of %s', metric._id);
    metric.lastUpdate = new Date(metric.lastUpdate);
    if (metricDef[metric._id].lastUpdate.getTime() >= metric.lastUpdate.getTime()) {
        console.log('%s already up to date.', metric._id);
        return;
    }
    if (metric._id in metricDef) {
        metric.cache = metricDef[metric._id].cache;
    } else {
        var now = new Date().getTime()/1000;
        metric.cache = {
            "raw": {
                "data": [], 
                "flushTime": now -600
            },
            "aggr": {
                "data": {
                    "avg": [],
                    "min": [],
                    "max": []
                },
                "flushTime": now - 21600
            }
        }; 
    }
    metricDef[metric._id] = metric;
};

function removeMetricDef(metric) {
    console.log('removing metricDef for %s', metric._id);
    delete metricDef[metric._id];
};

function processMetric(message) {
    var metric = JSON.parse(message.value);
    metric._id = util.format('%s.%s', metric.account, metric.name);
    //console.log("processing metric %s", metric._id);
    if (!(metric._id in metricDef)) {
        getMetricDef(metric, function(err, def) {
            if (err) {
                console.log('failed to get metricDef from DB.');
                console.log(err);
                return;
            }
            console.log('adding metricDef to cache.');

            //TDOD: pull metrics from influxdb and populate cache.
            var now = new Date().getTime()/1000;
            def.cache = {
                "raw": {
                    "data": [], 
                    "flushTime": now - 600
                },
                "aggr": {
                    "data": {
                        "avg": [],
                        "min": [],
                        "max": []
                    },
                    "flushTime": now - 26100
                }
            }; 
             if (!("thresholds" in def)) {
                def.thresholds = {};
            }
            ['warnMin', 'warnMax', 'criticalMin', 'criticalMax'].forEach(function(thresh) {
                if (!(thresh in def.thresholds)) {
                    def.thresholds[thresh] = null;
                }
            });
            metricDef[metric._id] = def;

            storeMetric(metric);
        });
    } else {
        storeMetric(metric);
    }
}

function getMetricDef(message, callback) {
    var filter = {
        _id: message._id
    }
    schema.metrics.model.findOne(filter).lean().exec(function(err, metric) {
        if (err) {
            console.log(err);
            return callback(err);
        }
        if (!(metric)) {
            console.log('%s not found. creating it.', message.name);
            message.lastUpdate = new Date();
            metric = schema.metrics.model(message);
            return metric.save(callback);
        }
        //TODO: load aggregation stats from backend.
        callback(null, metric);
    })
}

function storeMetric(metric) {
    if ((metric.value !== null) && (isNaN(metric.value) != true)) {
        buffer.metrics.push(
            util.format(
                '%s.%s %s %s',
                metric.account,
                metric.name, 
                metric.value,
                Math.floor(metric.time)
            )
        );
    }
    rollupRaw(metric);
    var now = new Date().getTime();
    if (buffer.lastFlush < (now - 1000)) {
        flushBuffer();
    }
    checkThresholds(metric);
}

function flushBuffer() {
    var payload = buffer.metrics;
    buffer = {
        lastFlush: new Date().getTime(),
        metrics: [],
    }

    carbon.write(payload, function(err) {
        if (err) {
            console.log('error submitting results to graphite');
            console.log(err);
            cb(err);
        } else {
            console.log('flushed %s metrics to carbon.', payload.length);
        }
    });
}

function rollupRaw(metric) {
    var def = metricDef[metric._id];
    if (def.cache.raw.flushTime < (metric.time - 300)) {
        
        if (def.cache.aggr.flushTime < (metric.time - 21600)) {
            var min, max, avg, sum;
            var count = def.cache.aggr.data.min.length;
            for (var i=0; i < count; i++) {
                if (min == undefined || def.cache.aggr.data.min[i] < min) {
                    min = def.cache.aggr.data.min[i];
                }
                if (max == undefined || def.cache.aggr.data.max[i] > max) {
                    min = def.cache.aggr.data.max[i];
                }
                if (def.cache.aggr.data.avg[i] != undefined) {
                    if (sum == undefined ) {
                        sum = def.cache.aggr.data.avg[i];
                    } else {
                        sum = sum + def.cache.aggr.data.avg[i];
                    }
                }
            }
            if (count > 0) {
                avg = sum /count;
            }
            def.cache.aggr.data.avg = [];
            def.cache.aggr.data.max = [];
            def.cache.aggr.data.min = [];
            def.cache.aggr.flushTime = metric.time;
            console.log("writing 6hour rollup for %s", metric._id);
            if (avg !== null && isNaN(avg) != true) {
                buffer.metrics.push(
                    util.format(
                        '6hour.avg.%s.%s %s %s',
                        metric.account,
                        metric.name, 
                        avg,
                        Math.floor(metric.time)
                    )
                );
            }
            if (max !== null && isNaN(max) != true) {
                buffer.metrics.push(
                    util.format(
                        '6hour.max.%s.%s %s %s',
                        metric.account,
                        metric.name, 
                        max,
                        Math.floor(metric.time)
                    )
                );
            }
            if (min !== null && isNaN(min) != true) {
                buffer.metrics.push(
                    util.format(
                        '6hour.min.%s.%s %s %s',
                        metric.account,
                        metric.name, 
                        min,
                        Math.floor(metric.time)
                    )
                );
            }
        }

        var min, max, avg, sum;
        var count = def.cache.raw.data.length;
        def.cache.raw.data.forEach(function(point) {
            if (min == undefined || point < min) {
                min = point;
            }
            if (max == undefined || point > max) {
                max = point;
            }
            if (sum == undefined) {
                sum = point;
            } else {
                sum = sum + point;
            }
        });
        if (count > 0) {
            avg = sum / count;
        }
        def.cache.raw.data = [];
        def.cache.raw.flushTime = metric.time
        if (avg !== null && isNaN(avg) != true) {
            buffer.metrics.push(
                util.format(
                    '5m.avg.%s.%s %s %s',
                    metric.account,
                    metric.name, 
                    avg,
                    Math.floor(metric.time)
                )
            );
        }
        if (max !== null && isNaN(max) != true) {
            buffer.metrics.push(
                util.format(
                    '5m.max.%s.%s %s %s',
                    metric.account,
                    metric.name, 
                    max,
                    Math.floor(metric.time)
                )
            );
        }
        if (min !== null && isNaN(min) != true) {
            buffer.metrics.push(
                util.format(
                    '5m.min.%s.%s %s %s',
                    metric.account,
                    metric.name, 
                    min,
                    Math.floor(metric.time)
                )
            );
        }
        
        def.cache.aggr.data.min.push(min);
        def.cache.aggr.data.max.push(max);
        def.cache.aggr.data.avg.push(avg);
    }
    if ((metric.value !== null) && (isNaN(metric.value) != true)) {
        def.cache.raw.data.push(metric.value);
    }
}

function checkThresholds(metric) {
    var def = metricDef[metric._id];
    var value = metric.value;
    var thresholds = def.thresholds;

    var state = 0;
    var msg = '';

    if (thresholds.criticalMin && value < thresholds.criticalMin) {
        msg = util.format('%s less than criticalMin %s', value, thresholds.criticalMin);
        console.log(msg);
        state = 2;
    }

    if (state < 2 && thresholds.criticalMax && value > thresholds.criticalMax) {
        msg = util.format('%s greater than criticalMax %s', value, thresholds.criticalMax);
        console.log(msg);
        state = 2;
    }

    if (state < 1 && thresholds.warnMin && value < thresholds.warnMin) {
        msg = util.format('%s greater than warnMin %s', value, thresholds.warnMin);
        console.log(msg);
        state = 1;
    }
    if (state < 1 && thresholds.warnMax && value > thresholds.warnMax) {
        msg = util.format('%s greater than warnMax %s', value, thresholds.warnMax);
        console.log(msg);
        state = 1;
    }

    var updates = false;
    var levelMap = ["ok", "warning", "critical"];
    var events = [];
    var currentState = def.state;
    if (state != def.state) {
        console.log('state has changed for %s. was %s now %s', def._id, currentState, state);
        def.state = state;
        def.lastUpdate = new Date(metric.time * 1000);
        updates = true;
    } else if ('keepAlives' in def && def.keepAlives && (def.lastUpdate.getTime()/1000) < metric.time - def.keepAlives) {
        console.log("no updates in %s seconds. sending keepAlive", def.keepAlives);
        updates = true;
        def.lastUpdate = new Date(metric.time * 1000);
        var checkEvent = {
            source: "metric",
            metric: metric.name,
            account: metric.account,
            parent: def.parent,
            type: 'keepAlive',
            state: levelMap[state],
            details: msg,
            timestamp: metric.time*1000,
        };
        events.push(checkEvent);
    }

    if (updates) {
        schema.metrics.model.findOne({_id: def._id}).lean().exec(function(err, obj) {
            if (err) {
                console.log("failed to get metric Object from DB.");
                console.log(err);
                return;
            }
            if (obj.lastUpdate.getTime()/1000 > metric.time) {
                console.log("MetricDef cache older then DB. %s", obj._id);
                delete metricDef[metric._id];
                return;
            }

            obj.lastUpdate = def.lastUpdate;
            obj.state = def.state;
            var id = obj._id;
            delete obj._id;
            schema.metrics.model.update({_id: id, lastUpdate: obj.lastUpdate}, obj, function(err, count) {
                if (err) {
                    console.log('error updating metric definition for %s', id)
                    console.log(err);
                    delete metricDef[id];
                    return;
                }
                if (count < 1) {
                    console.log('%s definition stale, update ignored and removing from cache.', id);
                    delete metricDef[id];
                    return;
                }
                console.log("%s update commited to DB.");
            });
        });
    }
    
    if (state > 0) {
        var checkEvent = {
            source: "metric",
            metric: metric.name,
            account: metric.account,
            parent: def.parent,
            type: 'checkFailure',
            state: levelMap[state],
            details: msg,
            timestamp: metric.time*1000,
        };
        events.push(checkEvent);
    }
    if (state != currentState) {
        var metricEvent = {
            source: "metric",
            metric: metric.name,
            account: metric.account,
            parent: def.parent,
            type: 'stateChange',
            state: levelMap[state],
            details: util.format('state transitioned from %s to %s', levelMap[currentState], levelMap[state]),
            timestamp: metric.time*1000,
        };
        events.push(metricEvent);
    }

    if (events.length > 0 ) {
        var messages = [];
        events.forEach(function(event) {
            messages.push(JSON.stringify(event));
        });
        producer.send([{topic: 'metricEvents', messages: messages}], function(err, data) {
            if (err) {
                console.log(err);
            }
        }); 
    }
}

process.on( "SIGINT", function() {
    console.log('CLOSING [SIGINT]');
    client.close();
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

    // every 60minutes, delete any expired (2days) metric from the DB.
    setInterval(function() {
        var filter = {
            lastUpdate: {$lt: new Date(new Date().getTime() - 172800000)},
        }
        schema.metrics.model.remove(filter).exec(function(err) {
            if (err) {
                console.log("Error deleting expired metrics.");
                console.log(err);
            }
        });
    }, 3600000);
} else {
    init();
}
