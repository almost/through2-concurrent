var through2Concurrent = require('./through2-concurrent');
var expect = require('expect.js');
var _ = require('underscore');

var oldNextTick = process.nextTick;


describe('through2-concurrent', function () {
  var nextTickScheduled, collectingThrough, transformCalls, flushCalls;

  function runNextTicks () {
    var execute = function (fn) {
        fn();
    };
    while (nextTickScheduled.length) {
      var fns = nextTickScheduled;
      nextTickScheduled = [];
      fns.forEach(execute);
    }
  }

  beforeEach(function () {
    nextTickScheduled = [];
    process.nextTick = function (fn) {
      nextTickScheduled.push(fn);
    };
  });

  describe('object streams', function () {
    beforeEach(function () {
      transformCalls = [];
      flushCalls = [];

      collectingThrough = through2Concurrent.obj(
        {maxConcurrency: 4},
        function (chunk, enc, callback) {
          transformCalls.push({chunk: chunk, enc: enc, callback: callback});
        },function (callback) {
          flushCalls.push({callback: callback});
        });
    });
    
    it('should initiially allow the maximum specified concurrency', function () {
      _.times(10, function (i) {
        collectingThrough.write({number: i});
      });
      runNextTicks();
      expect(transformCalls.length).to.be(4);
    });

    it('should feed more through once the concurrency drops below the given limit', function () {
      _.times(10, function (i) {
        collectingThrough.write({number: i});
      });
      runNextTicks();
      transformCalls[2].callback();
      transformCalls[0].callback();
      runNextTicks();
      expect(transformCalls.length).to.be(6);
    });

    it('should wait for all transform calls to finish before running flush', function () {
      _.times(10, function (i) {
        collectingThrough.write({number: i});
      });
      collectingThrough.end();
      runNextTicks();
      _.times(9, function (i) {
        transformCalls.pop().callback();
        runNextTicks();
      });
      expect(transformCalls.length).to.be(1);
      expect(flushCalls.length).to.be(0);
      transformCalls[0].callback();
      expect(flushCalls.length).to.be(1);
    });

    it('should wait for everything to complete before emitting "end"', function () {
      var endCalled = false;
      collectingThrough.on('end', function () {
        endCalled = true;
      });
      collectingThrough.resume();
      collectingThrough.write({hello: 'world'});
      collectingThrough.end();
      runNextTicks();
      expect(endCalled).to.be(false);
      transformCalls[0].callback();
      runNextTicks();
      expect(endCalled).to.be(false);
      flushCalls[0].callback();
      runNextTicks();
      expect(endCalled).to.be(true);
    });

    it('should pass down the stream data added with this.push', function () {
      var passingThrough = through2Concurrent.obj(
        {maxConcurrency: 1},
        function (chunk, enc, callback) {
          this.push({original: chunk});
          callback();
        },function (callback) {
          this.push({flushed: true});
        });
      var out = [];
      passingThrough.on('data', function (data) {
        out.push(data);
      });
      passingThrough.write("Hello");
      passingThrough.write("World");
      passingThrough.end();
      expect(out).to.eql([{original: "Hello"}, {original: "World"}, {flushed: true}]);
    });

    it('should pass down the stream data added as arguments to the callback', function () {
      var passingThrough = through2Concurrent.obj(
        {maxConcurrency: 1},
        function (chunk, enc, callback) {
          callback(null, {original: chunk});
        });
      var out = [];
      passingThrough.on('data', function (data) {
        out.push(data);
      });
      passingThrough.write("Hello");
      passingThrough.write("World");
      passingThrough.end();
      expect(out).to.eql([{original: "Hello"}, {original: "World"}]);
    });
  });
});
