

/**
 * Expose LinearBackoff
 */

module.exports = {
  Linear: LinearBackoff
};

/**
 * Linear backoff strategy
 */

function LinearBackoff(max) {
  this.max = max;
  this.reset();
}

/**
 * Reset backoff
 */

LinearBackoff.prototype.reset = function() {
  this.current = 0;
};

/**
 * Return next backoff value
 */

LinearBackoff.prototype.next = function() {
  var current;

  if (this.current >= this.max) {
    current = this.current;
  } else {
    current = this.current + 1;
  }

  this.current = current;

  return current;
};

