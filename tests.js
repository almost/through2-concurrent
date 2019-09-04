var through2Concurrent = require('./through2-concurrent');
var expect = require('expect.js');
var _ = require('underscore');

async function times(num, fn) {
  for (i = 0; i < num; i++) {
    const result = fn(i);
    if (result instanceof Promise) {
      await result;
    }
  }
}

var oldNextTick = process.nextTick;
function wait() {
  return new Promise((resolve) => {
    oldNextTick(() => {
      resolve();
    })
  })
}

describe('through2-concurrent', function () {
  var nextTickScheduled, collectingThrough, transformCalls, flushCalls, finalCalls;

  function runNextTicks () {
    var execute = function (arr) {
        var fn = arr[0];
        var args = arr[1];
        fn.apply(this, args);
    };
    while (nextTickScheduled.length) {
      var fns = nextTickScheduled;
      nextTickScheduled = [];
      fns.forEach(execute);
    }
    return wait();
  }

  beforeEach(function () {
    nextTickScheduled = [];
    process.nextTick = function (fn) {
      var exec = [fn, []];

      if (arguments.length > 1) {
        for (var i = 1; i < arguments.length; i++) {
          exec[1].push(arguments[i]);
        }
      }
      nextTickScheduled.push(exec);
    };
  });

  describe('object streams', function () {
    beforeEach(function () {
      transformCalls = [];
      flushCalls = [];
      finalCalls = [];

      var final = function(callback) {
        finalCalls.push({callback: callback});
      };
      collectingThrough = through2Concurrent.obj(
        {maxConcurrency: 4, final: final},
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

    it('should wait for all transform calls to finish before running final and flush when processing > concurrency', function (callback) {
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
      expect(finalCalls.length).to.be(0);
      expect(flushCalls.length).to.be(0);
      transformCalls[0].callback();

      // Ensure that flush is called straight after final even if we let the
      // event loop progress beforehand but not between the calls
      setTimeout(function() {
        expect(finalCalls.length).to.be(1);
        expect(flushCalls.length).to.be(0);
        finalCalls[0].callback();
        expect(finalCalls.length).to.be(1);
        expect(flushCalls.length).to.be(1);
        callback();
      }, 0);
    });

    it('should wait for all transform calls to finish before running final and flush when processing < concurrency', function (callback) {
      _.times(3, function (i) {
        collectingThrough.write({number: i});
      });
      collectingThrough.end();
      runNextTicks();
      _.times(2, function (i) {
        transformCalls.pop().callback();
        runNextTicks();
      });
      expect(transformCalls.length).to.be(1);
      expect(finalCalls.length).to.be(0);
      expect(flushCalls.length).to.be(0);
      transformCalls[0].callback();
      
      // Ensure that flush is called straight after final even if we let the
      // event loop progress beforehand but not between the calls
      setTimeout(function() {
        expect(finalCalls.length).to.be(1);
        expect(flushCalls.length).to.be(0);
        finalCalls[0].callback();
        expect(finalCalls.length).to.be(1);
        expect(flushCalls.length).to.be(1);
        callback();
      }, 0);
    });

    it('should wait for everything to complete before emitting "finish"', function (callback) {
      var finishCalled = false;
      collectingThrough.on('finish', function () {
        finishCalled = true;
      });
      
      collectingThrough.resume();
      collectingThrough.write({hello: 'world'});
      collectingThrough.write({hello: 'world'});
      collectingThrough.end();
      runNextTicks();
      expect(finishCalled).to.be(false);
      transformCalls[0].callback();
      runNextTicks();
      transformCalls[1].callback();
      runNextTicks();
      expect(finishCalled).to.be(false);
      finalCalls[0].callback();

      // runNextTicks does not set stream to sync=false, causing 'finish' to get stuck
      // because collectingThrough._writableState.pendingcb will always stay > 0
      // setImmediate will guarantee processing and possiblity of 'finish' event
      setImmediate(function() {
        expect(finishCalled).to.be(true);
        callback();
      });
    });

    it('should wait for everything to complete before emitting "end"', function () {
      var endCalled = false;
      collectingThrough.on('end', function () {
        endCalled = true;
      });
      collectingThrough.resume();
      collectingThrough.write({hello: 'world'});
      collectingThrough.write({hello: 'world'});
      collectingThrough.end();
      runNextTicks();
      expect(endCalled).to.be(false);
      transformCalls[0].callback();
      runNextTicks();
      transformCalls[1].callback();
      runNextTicks();
      expect(endCalled).to.be(false);
      finalCalls[0].callback();
      runNextTicks();
      expect(endCalled).to.be(false);
      flushCalls[0].callback();
      runNextTicks();
      expect(endCalled).to.be(true);
    });

    it('should pass down the stream data added with this.push', function () {
      var final = function(cb) {
        this.push({finished: true});
        cb();
      };
      var passingThrough = through2Concurrent.obj(
        {maxConcurrency: 1, final: final},
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
      runNextTicks();
      expect(out).to.eql([{original: "Hello"}, {original: "World"}, {finished: true}, {flushed: true}]);
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
      
    describe('without a flush argument or final option', function () {
      beforeEach(function () {
        transformCalls = [];
        flushCalls = [];
        
        collectingThrough = through2Concurrent.obj(
          {maxConcurrency: 4},
          function (chunk, enc, callback) {
            transformCalls.push({chunk: chunk, enc: enc, callback: callback});
          });
      });
      
      it('should wait for everything to complete before emitting "finish"', function (callback) {
        var finishCalled = false;
        collectingThrough.on('finish', function () {
          finishCalled = true;
        });
        
        collectingThrough.resume();
        collectingThrough.write({hello: 'world'});
        collectingThrough.end();
        runNextTicks();
        expect(finishCalled).to.be(false);
        transformCalls[0].callback();
  
        // runNextTicks does not progress ticks and set stream to sync=false, causing
        // 'finish' to never fire because collectingThrough._writableState.pendingcb
        // will always stay > 0 setImmediate will guarantee processing of 'finish' event
        setImmediate(function() {
          expect(finishCalled).to.be(true);
          callback();
        });
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
        expect(endCalled).to.be(true);
      });
    });
  });

  describe('object streams (preserveOrder=true)', function () {
    beforeEach(function () {
      transformCalls = [];
      flushCalls = [];
      finalCalls = [];

      var final = function (callback) {
        finalCalls.push({ callback: callback });
      };
      collectingThrough = through2Concurrent.obj(
        { maxConcurrency: 4, final: final, preserveOrder: true },
        function (chunk, enc, callback) {
          transformCalls.push({ chunk: chunk, enc: enc, callback: callback });
        }, function (callback) {
          flushCalls.push({ callback: callback });
        });
    });

    it('should initiially allow the maximum specified concurrency', function () {
      _.times(10, function (i) {
        collectingThrough.write({ number: i });
      });
      runNextTicks();
      expect(transformCalls.length).to.be(4);
    });

    it('(preserveOrder=true)should not feed more through once the concurrency drops below the given limit', function () {
      _.times(10, function (i) {
        collectingThrough.write({ number: i });
      });
      runNextTicks();
      transformCalls[2].callback();
      transformCalls[0].callback();
      runNextTicks();
      expect(transformCalls.length).to.be(4);
    });

    it('should wait for all transform calls to finish before running final and flush when processing > concurrency', async function () {
      times(10, function (i) {
        collectingThrough.write({ number: i });
      });
      collectingThrough.end();
      await runNextTicks();
      await times(9, async function (i) {
        const poped = transformCalls.pop()
        poped.callback();
        await runNextTicks();
      });

      expect(transformCalls.length).to.be(1);
      expect(finalCalls.length).to.be(0);
      expect(flushCalls.length).to.be(0);
      transformCalls[0].callback();

      // Ensure that flush is called straight after final even if we let the
      // event loop progress beforehand but not between the calls
      await wait();
      await runNextTicks();

      expect(finalCalls.length).to.be(1);
      expect(flushCalls.length).to.be(0);
      finalCalls[0].callback();
      expect(finalCalls.length).to.be(1);
      expect(flushCalls.length).to.be(1);
    });

    it('should wait for all transform calls to finish before running final and flush when processing < concurrency', async function () {
      _.times(3, function (i) {
        collectingThrough.write({ number: i });
      });
      collectingThrough.end();
      await runNextTicks();
      _.times(2, async function (i) {
        transformCalls.pop().callback();
        await runNextTicks();
      });
      expect(transformCalls.length).to.be(1);
      expect(finalCalls.length).to.be(0);
      expect(flushCalls.length).to.be(0);
      transformCalls[0].callback();

      await wait();
      await runNextTicks();
      // Ensure that flush is called straight after final even if we let the
      // event loop progress beforehand but not between the calls
      expect(finalCalls.length).to.be(1);
      expect(flushCalls.length).to.be(0);
      finalCalls[0].callback();
      expect(finalCalls.length).to.be(1);
      expect(flushCalls.length).to.be(1);
    });

    it('should wait for everything to complete before emitting "finish"', async function () {
      var finishCalled = false;
      collectingThrough.on('finish', function () {
        finishCalled = true;
      });
      collectingThrough.resume();
      collectingThrough.write({ hello: 'world' });
      collectingThrough.write({ hello: 'world' });
      collectingThrough.end();
      await runNextTicks();
      expect(finishCalled).to.be(false);
      transformCalls[0].callback();
      await runNextTicks();
      transformCalls[1].callback();
      await runNextTicks();
      expect(finishCalled).to.be(false);
      await runNextTicks();
      finalCalls[0].callback();
      // runNextTicks does not set stream to sync=false, causing 'finish' to get stuck
      // because collectingThrough._writableState.pendingcb will always stay > 0
      // setImmediate will guarantee processing and possiblity of 'finish' event
      await wait();
      expect(finishCalled).to.be(true);
    });

    it('should wait for everything to complete before emitting "end"', async function () {
      var endCalled = false;
      collectingThrough.on('end', function () {
        endCalled = true;
      });
      collectingThrough.resume();
      collectingThrough.write({ hello: 'world' });
      collectingThrough.write({ hello: 'world' });
      collectingThrough.end();
      await runNextTicks();
      expect(endCalled).to.be(false);
      transformCalls[0].callback();
      await runNextTicks();
      transformCalls[1].callback();
      await runNextTicks();
      expect(endCalled).to.be(false);

      await runNextTicks();
      finalCalls[0].callback();
      await runNextTicks();
      expect(endCalled).to.be(false);
      flushCalls[0].callback();
      await runNextTicks();
      expect(endCalled).to.be(true);
    });

    it('should pass down the stream data added with this.push', function (done) {
      var final = function (cb) {
        this.push({ finished: true });
        cb();
      };
      const curNextick = process.nextTick;
      process.nextTick = oldNextTick;
      var passingThrough = through2Concurrent.obj(
        { maxConcurrency: 1, final: final },
        function (chunk, enc, callback) {
          this.push({ original: chunk });
          callback();
        }, function (callback) {
          this.push({ flushed: true });
        });
      var out = [];
      passingThrough.on('data', function (data) {
        out.push(data);
      });
      passingThrough.write("Hello");
      passingThrough.write("World");
      passingThrough.end();
      setTimeout(() => {
        expect(out).to.eql([{ original: "Hello" }, { original: "World" }, { finished: true }, { flushed: true }]);
        process.nextTick = curNextick;
        done();
      }, 0)
    });

    it('should pass down the stream data added as arguments to the callback', async function () {
      var passingThrough = through2Concurrent.obj(
        { maxConcurrency: 1 },
        function (chunk, enc, callback) {
          callback(null, { original: chunk });
        });
      var out = [];
      passingThrough.on('data', function (data) {
        out.push(data);
      });
      passingThrough.write("Hello");
      passingThrough.write("World");
      await wait();
      passingThrough.end();
      await runNextTicks();
      expect(out).to.eql([{ original: "Hello" }, { original: "World" }]);
    });

    describe('without a flush argument or final option', function () {
      beforeEach(function () {
        transformCalls = [];
        flushCalls = [];

        collectingThrough = through2Concurrent.obj(
          { maxConcurrency: 4 },
          function (chunk, enc, callback) {
            transformCalls.push({ chunk: chunk, enc: enc, callback: callback });
          });
      });

      it('should wait for everything to complete before emitting "finish"', async function () {
        var finishCalled = false;
        collectingThrough.on('finish', function () {
          finishCalled = true;
        });

        collectingThrough.resume();
        collectingThrough.write({ hello: 'world' });
        collectingThrough.end();
        runNextTicks();
        expect(finishCalled).to.be(false);
        transformCalls[0].callback();

        // runNextTicks does not progress ticks and set stream to sync=false, causing
        // 'finish' to never fire because collectingThrough._writableState.pendingcb
        // will always stay > 0 setImmediate will guarantee processing of 'finish' event
        await wait();
        await runNextTicks();
        expect(finishCalled).to.be(true);
      });

      it('should wait for everything to complete before emitting "end"', async function () {
        var endCalled = false;
        collectingThrough.on('end', function () {
          endCalled = true;
        });
        collectingThrough.resume();
        collectingThrough.write({ hello: 'world' });
        collectingThrough.end();
        await runNextTicks();
        expect(endCalled).to.be(false);
        transformCalls[0].callback();
        await runNextTicks();
        await runNextTicks();
        expect(endCalled).to.be(true);
      });
    });
  });

});
