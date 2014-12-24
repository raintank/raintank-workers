'use strict';

var config = require('./config').config;
var schema = require('raintank-core/schema');
var util = require('util');
var carbon = require('raintank-core/lib/carbon');
var kafka = require('kafka-node');
var Consumer = kafka.Consumer;
var Offset = kafka.Offset;
var producer = require('raintank-core/lib/kafka').producer;
var hashCode = require('string-hash');
var async = require('async');
var numCPUs = config.numCPUs;
var http = require('http');
var cluster = require('cluster');
var url = require('url');
var zlib = require('zlib');

function handler (req, res) {
  res.writeHead(404);
  res.end();
}

if (cluster.isMaster) {
  // Fork workers.
  for (var i = 0; i < numCPUs; i++) {
    cluster.fork();
  }
  cluster.on('exit', function(worker, code, signal) {
    console.log('worker ' + worker.process.pid + ' died');
  });

} else {
    var app = http.createServer(handler)
    var io = require('socket.io')(app);

    app.listen(process.env.PORT || 8181);


    var RECV = 0;
    var PUB = 0;
    var BUFFER = {};

    setInterval(function() {
        var messages = BUFFER;
        BUFFER = {};
        var count = 0;
        var kafkaPayload = [];
        for (var id in messages) {
            kafkaPayload.push( {
                topic: "metrics",
                messages: messages[id],
                partition: id
            });
            count = count + messages[id].length;
        }
        if (kafkaPayload.length < 1) {
            return;
        }
        producer.send(kafkaPayload, function(err, resp) {
            if (err) {
                console.log("error sending serviceEvent to Kafka.");
                console.log(err);
                return;
            }
            PUB = PUB + count;
        });
    }, 100);
        

    setInterval(function() {
        var recv = RECV;
        RECV=0;
        var pub = PUB;
        PUB = 0;
        console.log("RECV:%s, PUB:%s metrics per sec", recv/10, pub/10);
    }, 10000);


    io.use(function(socket, next) {
        var req  = url.parse(socket.request.url, true);
        //TODO: handle real user authentication so clients can deploy their own
        // collector nodes.
        if ('token' in req.query && req.query.token == config.adminToken) {
            socket.request.user = '531307ac1133a83d285b05a1';
            socket.request.locationCode = req.query.location;
            next();
        } else {
            console.log('connection attempt not authenticated.');
            next(new Error('Authentication error'));
        }
    });

    io.on('connection', function(socket) {
        console.log('new connection for user: %s@%s', socket.request.user, socket.request.locationCode);
        socket.join(socket.request.locationCode);

        socket.on('serviceEvent', function(data) {
            zlib.inflate(data, function(err, buffer) {
                if (err) {
                    console.log("failed to decompress payload.");
                    console.log(err);
                    return;
                }
                var payload = buffer.toString();
                producer.send([{topic: 'serviceEvents', messages: [payload]}], function(err, resp) {
                    if (err) {
                        console.log("error sending serviceEvent to Kafka.");
                        console.log(err);
                    } else {
                        console.log('events submitted successfully to kafka.');
                    }
                });
            });
        });

        socket.on('results', function(data) {
            zlib.inflate(data, function(err, buffer) {
                if (err) {
                    console.log("failed to decompress payload.");
                    console.log(err);
                    return;
                }
                var payload = JSON.parse(buffer.toString());
                var count =0;
                payload.forEach(function(metric) {
                    count++;
                    var partition = hashCode(metric.name) % config.kafka.partitions;
                    if (!(partition in BUFFER)) {
                        BUFFER[partition] = [];
                    }
                    BUFFER[partition].push(JSON.stringify(metric));
                });
                RECV = RECV + count;
            });
        });
        var filter = {
            deletedAt: {$exists: false},
            locations: socket.request.locationCode,
        }
        schema.services.model.find(filter).lean().exec(function(err, services){
            socket.emit('refresh', JSON.stringify(services));
        });
    });

    setInterval(function() {
        schema.locations.model.find().lean().select('_id').exec(function(err, locations) {
            if (err) {
                console.log("ERROR getting location list.");
                console.log(err);
                return;
            }
            locations.forEach(function(loc) {
                var filter = {
                    deletedAt: {$exists: false},
                    locations: loc._id,
                }
                schema.services.model.find(filter).lean().exec(function(err, services){
                    console.log("refreshing serviceList for %s", loc._id);
                    io.to(loc._id).emit('refresh', JSON.stringify(services));
                });
            });
        });
    }, 300000);

    var running = false;
    var client;
    var init = function() {
        client = new kafka.Client(config.kafka.connectionString, 'serviceChange', {sessionTimeout: 1500});
        running = true;
        var topics = [];
        var i =0;
        while (i < config.kafka.partitions) {
            topics.push({topic: "serviceChange", partition: i});
            i++;
        }
        console.log(topics);
        var offset = new Offset(client);
        var payload = [];
        topics.forEach(function(topic) {
            payload.push({
                topic: topic.topic,
                partition: topic.partition,
                time: -1,
            });
        });

        var steps = [];
        payload.forEach(function(p) {
            steps.push(function(cb) {
                offset.fetch([p], function(err, data) {
                    var topic = {
                        topic: p.topic,
                        partiton: p.partition,
                        offset: 0
                    };
                    if (err) {
                        console.log("Error when fetching topic offset.");
                        console.log(err);
                    } else {
                        console.log("got offset %j", data);
                        topic.offset = data[p.topic][p.partition];
                    }
                    cb(null, topic);
                });
            });
        });
        async.parallel(steps, function(err, topics) {
            console.log("TOPICS: %j", topics);
            var consumer = new Consumer(
                client,
                topics,
                {
                    groupId: "serviceChange-"+process.pid,
                    autoCommitIntervalMs: 1000,
                    // The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
                    fetchMaxBytes: 1024 * 10,
                    fromOffset: true,
                }
            );
            consumer.on('error', function(err) {
                console.log('consumer emited error.');
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
                var payload = JSON.parse(message.value);
                var action = payload.action;
                console.log("got new message.");
                console.log(message.value);
                payload.service.locations.forEach(function(loc) {
                    io.to(loc).emit(action, JSON.stringify(payload.service));
                });
            });
        });
    }

    init();
}
