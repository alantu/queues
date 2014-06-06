var queues = require('..');

p = queues.iron({
  "token":"xxx",
  "project_id":"xxx"
});

q = p.get('operations');

q.post({a:1});
