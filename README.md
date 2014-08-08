queues
======

Collection of queue wrappers with a common interface.

For IronMQ:

```javascript
var queues = require('queues');

var provider = queues.iron(config);

var q = provider.get('notifications');

// set a backoff strategy
q.idleBackoff('linear', 5);

q.connect();

q.on('message', function(msg) {
  // handle message
});

q.post({ arg1: 'hello' });
```
