/**
 * Module dependencies
 */

var Emitter = require('event-emitter');
var ironMQ = require('iron_mq');
var debug = require('debug')('queues');

/**
 * Export module
 */

module.exports = Provider;

/**
 * IronMQ provider
 *
 * @class Provider
 */

function Provider(config) {
  if (!(this instanceof Provider)) {
    return new Provider(config);
  }


  this.client = new ironMQ.Client(config);
  this.queues = {};
}

/**
 * Get a queue instance
 *
 * @api public
 */

Provider.prototype.get = function(name) {
  this.queues[name] = this.queues[name] || new Queue(this.client, name);

  return this.queues[name];
};

/**
 * @class Queue
 *
 * @param {Object} config - ironmq config object
 */

function Queue(client, name) {
  this.name = name;
  this.q = client.queue(name);
}

Emitter(Queue.prototype);

/**
 * Connect to the queue 
 */

Queue.prototype.connect = function() {
  this.pull();
};

/**
 * Pull messages from the queue
 *
 * @api private
 */

Queue.prototype.pull = function() {
  this.q.get({ n: 5 }, this.onMessage.bind(this));
};

/**
 * Handler for message received
 */

Queue.prototype.onMessage = function(err, body) {
  var self = this;

  debug('message received on queue %s %o', this.name, body);

  if (err) {
    debug('error: %o', err);
    this.emit('error', err);
  }

  body.forEach(function(msg){
    self.emit('message', msg);

    self.q.del(msg.id, function(err, body) {
      if (err) 
        debug('could not delete "%s"', msg.id);
    });
  });

  this.pull();
};

/**
 * Post a message to the queue
 */

Queue.prototype.post = function(msg, fn) {
  this.q.post(JSON.stringify(msg), function() {
    debug('message posted');
    fn && fn();
  });
};

