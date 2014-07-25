
/**
 * Module dependencies
 */

var Emitter = require('event-emitter');
var ironMQ = require('iron_mq');
var debug = require('debug')('queues:iron');

/**
 * Export module
 */

module.exports = Provider;

/**
 * IronMQ provider
 *
 * @class Provider
 */

function Provider(id, secret) {
  if (!(this instanceof Provider)) {
    return new Provider(id, secret);
  }

  this.client = new ironMQ.Client({
    project_id: id, 
    token: secret
  });

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
    return;
  }

  debug("body: ", body);

  if (body) {
    body.forEach(function(msg){
      self.emit('message', msg);
    });
  }

  this.pull();
};

/**
 * Post a message to the queue
 */

Queue.prototype.post = function(msg, callback) {
  this.q.post(JSON.stringify(msg), function() {
    debug('message posted');
    callback && callback();
  });
};

/**
 * Delete a message from the queue
 */

Queue.prototype.remove = function(msg, callback) {
  var self = this;
  var msgId = msg.id;

  self.q.del(msgId, function(err, body) {
    if (err)
      debug('could not delete "%s"', msgId);
    else
      debug('message deleted "%s"', msgId);
  });
};
