'use strict';

var queue = require('raintank-queue');

var broker = new queue.Broker({
	consumerSocketAddr: 'tcp://0.0.0.0:9998', //address that consumers will connect to.
	publisherSocketAddr: 'tcp://0.0.0.0:9997', //address that publishers will connect to.
	mgmtUrl: "http://0.0.0.0:9999", //port the management Socket.io server should listen on.
	flushInterval: 100, //how long to buffer messages before sending to comsumers.
	partitions: 10 //how man partitions the topic should be split into.
});

