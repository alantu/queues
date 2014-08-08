
/**
 * Module dependencies
 */

var Emitter = require('event-emitter');
//var SQS = require('aws-sqs');
var AWS = require('aws-sdk');
var debug = require('debug')('queues:sqs');
var Backoff = require('./backoff');

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

  this.client = new AWS.SQS({
    accessKeyId: id,
    secretAccessKey: secret,
    region: 'us-east-1'
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
  
  debug('connecting to', this.name, this.client);

  this.client.createQueue({ 
    QueueName: this.name
  }, function(err, res) {
      if (err) {
        debug('Failed to initialize queue', err);
        throw new Error(err);
      }

      var id = res.QueueUrl;

      // initialize queue
      self.init(id);

      debug('connected to queue %s', id);

      self.emit('connected');
    }
  );
};

/**
 * Define backoff strategy
 *
 * @param {String} type - linear, exponential.
 * @param {Integer} max - max seconds backoff.
 */

Queue.prototype.idleBackoff = function(type, max) {
  var backoff;

  if (type === 'linear') {
    backoff = new Backoff.Linear(max);
  } else {
    throw new Error('invalid idle backoff strategy');
  }

  this._idleBackoff = backoff;

  return this;
};

/**
 * Pull messages from the queue
 *
 * @api private
 */

Queue.prototype.pull = function() {
  debug('polling queue');

  this.client.receiveMessage({ 
    QueueUrl: this.id,
    MaxNumberOfMessages: 10,
    WaitTimeSeconds: 20
  }, this.onMessage.bind(this));
};

/**
 * Start consuming the queue.
 *
 * @api public
 */

Queue.prototype.start = function() {
  if (this._started) {
    return;
  }

  this._started = true;
  this.pull();
};

/**
 * Check wether the queue is started or not
 *
 * @api public
 */

Queue.prototype.isStarted = function() {
  return !!this._started;
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

  debug('received messages %j', body);

  var messages = body.Messages || [];
  var count = messages.length;

  if (messages) {
    debug('received %s messages', count);
    messages.forEach(function(msg){
      self.emit('message', msg);
    });
  }

  if (this._idleBackoff) {
    if (!count) {
      var secs = this._idleBackoff.next();
      debug('idle, backing-off %s seconds', secs);

      setTimeout(this.pull.bind(this), secs*1000);
    } else {
      this._idleBackoff.reset();
      this.pull();
    }
  } else {
    this.pull();
  }
};

/**
 * Post a message to the queue
 *
 * @api public
 */

Queue.prototype.post = function(msg, fn) {
  var params = {
    QueueUrl: this.id,
    MessageBody: JSON.stringify(msg)
  };

  debug('posting message %j', params, this.id);

  this.client.sendMessage(params, function(err, res) {
      if (err) {
        debug('error posting message %j', err);
      } else {
        debug('message posted ok');
      }

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

  this.client.deleteMessage({
    QueueUrl: this.id,
    ReceiptHandle: receipt
  }, function(err, body) {
    if (err)
      debug('could not delete "%s"', receipt, err);
    else
      debug('message deleted');
  });
};

/**
// MAIN 
var creds = {
  "id": "XXX",
  "secret": "XXX"
};

var provider = new Provider(creds.id, creds.secret);
var queue = provider.get('dev-notifications');

queue.on('connected', function() {
  for (var i=0; i<10; i++) {
    queue.post('hello!', function(err, res) {
      if (err)
        debug('error posting callback %j, %j', err, res);
      else
        debug('posted message with id', res.MessageId);
    });
  }

  queue.start();
});

queue.idleBackoff('linear', 5);

queue.on('message', function(msg) {
  debug('message received', msg.Body);
  queue.remove(msg);
});

queue.connect();
**/
