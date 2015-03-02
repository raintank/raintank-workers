'use strict';
var config = require('./config').config;
var util = require('util');
var carbon = require('./lib/carbon');
var MetricDefinitions = require("./lib/metricDefinitions");
var queue = require('raintank-queue');
var cluster = require('cluster');

var numCPUs = config.numCPUs;

var metricDef = {};
var publisher;
function init() {
    console.log("starting metricDefConsumer");
    var metricDefConsumer = new queue.Consumer({
        url: config.queue.url,
        exchangeName: "metrics",  //this should match the name of the exchange the producer is using.
        exchangeType: "topic", // this should match the exchangeType the producer is using.
        queueName: '', //leave blank for an auto generated name. recommended when creating an exclusive queue.
        exclusive: true, //make the queue exclusive.
        durable: false,
        autoDelete: true,
        queuePattern: 'metrics.*', //match metrics.create, metrics.update, metrics.remove
        retryCount: -1, // keep trying to connect forever.
        handler: processMetricDefEvent
    });
    metricDefConsumer.on('error', function(err) {
        console.log("metricDefConsumer emitted fatal error.")
        console.log(err);
        process.exit(1);
    });
    console.log("starting metricResultsConsumer");
    var metricResultsConsumer = new queue.Consumer({
        url: config.queue.url,
        exchangeName: "metricResults",  //this should match the name of the exchange the producer is using.
        exchangeType: "x-consistent-hash", // this should match the exchangeType the producer is using.
        queueName: '', //leave blank for an auto generated name. recommended when creating an exclusive queue.
        exclusive: true, //make the queue exclusive.
        durable: false,
        autoDelete: true,
        queuePattern: '10', //
        retryCount: -1, // keep trying to connect forever.
        handler: processMetrics
    });
    metricResultsConsumer.on('error', function(err) {
        console.log("metricResultsConsumer emitted fatal error.")
        console.log(err);
        process.exit(1);
    });
    console.log("initializing metricEvents publisher.");
    publisher = new queue.Publisher({
        url: config.queue.url,
        exchangeName: "metricEvents",
        exchangeType: "fanout",
        retryCount: 5,
        retryDelay: 1000,
    });
}

var buffer = {
    lastFlush: new Date().getTime(),
    metrics: [],
}


function processMetricDefEvent(message) {
    var routingKey = message.fields.routingKey;
    var action = routingKey.split('.')[1]; //one of update, create, delete.
    if (action === 'update') {
        updateMetricDef(JSON.parse(message.content.toString()));
    } else if (action === 'remove') {
        removeMetricDef(JSON.parse(message.content.toString()));
    } else {
        console.log("messsage has unknown action. ", action);
    }
}

function updateMetricDef(metric) {
    if (!(metric.id in metricDef)) {
        return;
    }
    console.log('updating metricDef of %s', metric.id);
    metric.lastUpdate = new Date(metric.lastUpdate).getTime();
    if (metricDef[metric.id].lastUpdate >= metric.lastUpdate) {
        console.log('%s already up to date.', metric.id);
        return;
    }
    if (metric.id in metricDef) {
        metric.cache = metricDef[metric.id].cache;
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
    metricDef[metric.id] = metric;
};

function removeMetricDef(metric) {
    console.log('removing metricDef for %s', metric.id);
    delete metricDef[metric.id];
};

function processMetrics(message) {
    var metrics = JSON.parse(message.content.toString());
    metrics.forEach(function(metric) {
        metric.id = util.format('%s.%s', metric.org_id, metric.name);
        //console.log("processing metric %s", metric.id);
        if (!(metric.id in metricDef)) {
            getMetricDef(metric, function(err, def) {
                if (err) {
                    console.log('failed to get metricDef from DB.');
                    console.log(err);
                    return;
                }
                console.log('adding metricDef to cache.', def.id);

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
                metricDef[metric.id] = def;

                storeMetric(metric);
            });
        } else {
            storeMetric(metric);
        }
    });
}

function getMetricDef(message, callback) {
    MetricDefinitions.get(message.id, function(err, metric) {
        if (err) {
            console.log(err);
            return callback(err);
        }
        if (!(metric)) {
            console.log('%s not found. creating it.', message.name);
            message.lastUpdate = new Date().getTime();
            if (!("thresholds" in message)) {
                message.thresholds = {};
            }
            ['warnMin', 'warnMax', 'criticalMin', 'criticalMax'].forEach(function(thresh) {
                if (!(thresh in message.thresholds)) {
                    message.thresholds[thresh] = null;
                }
            });
            metric = new MetricDefinitions.Model(message);
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
                metric.org_id,
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
    var def = metricDef[metric.id];
    if (def.cache.raw.flushTime < (metric.time - 600)) {
        
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
            if (avg !== null && isNaN(avg) != true) {
                console.log("writing 6hour rollup for %s", metric.id);
                buffer.metrics.push(
                    util.format(
                        '6hour.avg.%s.%s %s %s',
                        metric.org_id,
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
                        metric.org_id,
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
                        metric.org_id,
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
            console.log("writing 10min rollup for %s:%s", metric.id, avg);
            buffer.metrics.push(
                util.format(
                    '10m.avg.%s.%s %s %s',
                    metric.org_id,
                    metric.name, 
                    avg,
                    Math.floor(metric.time)
                )
            );
        }
        if (max !== null && isNaN(max) != true) {
            buffer.metrics.push(
                util.format(
                    '10m.max.%s.%s %s %s',
                    metric.org_id,
                    metric.name, 
                    max,
                    Math.floor(metric.time)
                )
            );
        }
        if (min !== null && isNaN(min) != true) {
            buffer.metrics.push(
                util.format(
                    '10m.min.%s.%s %s %s',
                    metric.org_id,
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
    var def = metricDef[metric.id];
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
        console.log('state has changed for %s. was %s now %s', def.id, currentState, state);
        def.state = state;
        def.lastUpdate = new Date(metric.time * 1000).getTime();
        updates = true;
    } else if ('keepAlives' in def && def.keepAlives && (def.lastUpdate/1000) < metric.time - def.keepAlives) {
        console.log("no updates in %s seconds. sending keepAlive", def.keepAlives);
        updates = true;
        def.lastUpdate = new Date(metric.time * 1000).getTime();
        var checkEvent = {
            source: "metric",
            metric: metric.name,
            account: metric.org_id,
            parent: def.parent,
            type: 'keepAlive',
            state: levelMap[state],
            details: msg,
            timestamp: metric.time*1000,
        };
        events.push(checkEvent);
    }

    if (updates) {
        def.save(function(err) {
            if (err) {
                console.log('error updating metric definition for %s', def.id)
                console.log(err);
                delete metricDef[def.id];
                return;
            }
            console.log("%s update commited to elasticSearch.", def.id);
        });
    }

    if (state > 0) {
        var checkEvent = {
            source: "metric",
            metric: metric.name,
            account: metric.org_id,
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
            account: metric.org_id,
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
        events.forEach(function(e) {
            publisher.publish(e, e.type, function(err) {
                if (err) {
                    console.log("failed to publish event.", err);
                }
            });
        });
    }
}

process.on( "SIGINT", function() {
    console.log('CLOSING [SIGINT]');
    process.exit();
});

if (cluster.isMaster) {
    // Fork workers.
    initElasticsearch();
    for (var i = 0; i < numCPUs; i++) {
        cluster.fork();
    }
    cluster.on('exit', function(worker, code, signal) {
        console.log('worker ' + worker.process.pid + ' died. Restarting....');
        //cluster.fork();
    });
} else {
    init();
}


function initElasticsearch() {
    var client = MetricDefinitions.getClient();
    client.indices.exists({
        index: "definitions"
    }, function(err, exists) {
        if (err) {
            console.log("index exists error: ", err);
            process.exit(1);
        }
        if (exists) {
            client.indices.putMapping({
                index: "definitions",
                type: "metric",
                body: {
                    metric: {
                        properties: {
                            name: {type: "string", "index": "not_analyzed"}
                        }
                    }
                }
            });
        } else {
            client.indices.create({
                index: "definitions",
                body: {
                    mappings: {
                        metric: {
                            properties: {
                                name: {type: "string", "index": "not_analyzed"}
                            }
                        }
                    }
                }
            }, function(err) {
                if (err) {
                    console.log("failed to create definitions index.", err);
                    process.exit(1);
                }
                
            });
        }
    });
}
