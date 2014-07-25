
/**
 * Module dependencies
 */

var Emitter = require('event-emitter');
var SQS = require('aws-sqs');
var debug = require('debug')('queues:sqs');

/**
 * Export module
 */

module.exports = Provider;

/**
 * AWS SQS provider
 *
 * @class Provider
 */

function Provider(id, secret) {
  if (!(this instanceof Provider)) {
    return new Provider(id, secret);
  }

  this.client = new SQS(id, secret);
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
  this.client = client;
}

Emitter(Queue.prototype);

/**
 * Initialize the queue
 *
 * @api private
 */

Queue.prototype.init = function(id) {
  debug('initializing queue ', id);
  
  // queue id
  this.id = id;
};

/**
 * Connect to the queue
 *
 * @api public
 */

Queue.prototype.connect = function() {
  var self = this;
  
  this.client.createQueue(this.name, {}, 
    function(err, res) {
      if (err) {
        throw new Error('Failed to initialize queue');
      }


      // initialize queue
      self.init(res);

      debug('connected, waiting for messages..');

      self.emit('connected');
      self.pull();
    }
  );
};

/**
 * Pull messages from the queue
 *
 * @api private
 */

Queue.prototype.pull = function() {
  this.client.receiveMessage(this.id, { 
    maxNumberOfMessages: 5 
  }, this.onMessage.bind(this));
};

/**
 * Handler for message received
 *
 * @api private
 */

Queue.prototype.onMessage = function(err, body) {
  var self = this;

  if (err) {
    debug('error: %o', err);
    this.emit('error', err);
    return;
  }

  if (body) {
    body.forEach(function(msg){
      self.emit('message', msg);
    });
  }

  this.pull();
};

/**
 * Post a message to the queue
 *
 * @api public
 */

Queue.prototype.post = function(msg, fn) {
  debug('posting message');

  this.client.sendMessage(this.id, JSON.stringify(msg), null,
    function(err, res) {
      debug('message posted');
      fn && fn(err, res);
    }
  );
};

/**
 * Delete a message from the queue
 *
 * @param {Object} msg - the full received object
 */

Queue.prototype.remove = function(msg, fn) {
  var receipt = msg.ReceiptHandle;

  this.client.deleteMessage(this.id, receipt, function(err, body) {
    if (err)
      debug('could not delete "%s"', receipt, err);
    else
      debug('message deleted');
  });
};

// MAIN 
//var creds = {
//  id: 'XXX',
//  secret: 'YYY'
//};
//
//var provider = new Provider(creds.id, creds.secret);
//
//var queue = provider.get('dev-notifications');
//
//queue.on('connected', function() {
//  queue.post('hello!', function(err, res) {
//    if (err)
//      debug('error posting callback %j, %j', err, res);
//    else
//      debug('posted message with id', res.MessageId);
//  });
//});
//
//queue.on('message', function(msg) {
//  debug('message received', msg.Body);
//  queue.remove(msg);
//});
//
//queue.connect();

