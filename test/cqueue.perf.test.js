const { describe, it, before } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execQueue, setLogFunction } = require('../cqueue.js');

/**
 * Performance tests: bounds are deliberately generous to stay reliable on slow CI runners,
 * while still failing by a wide margin on the performance regressions they protect against:
 * - the ~1ms setTimeout floor between each element (fixed with setImmediate when delay is 0)
 * - the O(n²) cost of Array.shift() on large chunks (fixed with a cursor)
 */
setLogFunction(() => {});

const PERF_OPTIONS = { logEnabled: false, logQueueStatus: false, retry: 0 };

function execQueueP (queueName, list, functionToExecute, options) {
  return new Promise((resolve, reject) => {
    execQueue(queueName, list, functionToExecute, options, (err, results, errors) => {
      return err ? reject(err) : resolve({ results, errors });
    });
  });
}

before(() => {
  const _tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cqueue-perf-'));
  process.chdir(_tmp);
});

describe('performances', () => {
  it('should process 5 000 synchronous elements on a single queue in less than 2 seconds', async () => {
    const _list = Array.from({ length: 5000 }, (_, i) => i);
    const _start = Date.now();
    const { results } = await execQueueP('perf-single', _list, (el, next) => next(null, el), Object.assign({ concurrency: 1 }, PERF_OPTIONS));
    const _duration = Date.now() - _start;
    assert.strictEqual(results.length, 5000);
    assert.ok(_duration < 2000, `took ${_duration}ms, expected < 2000ms (~${(_duration / 5000).toFixed(3)}ms/element)`);
  });

  it('should process 20 000 elements across 4 queues in less than 3 seconds', async () => {
    const _list = Array.from({ length: 20000 }, (_, i) => i);
    const _start = Date.now();
    const { results, errors } = await execQueueP('perf-multi', _list, (el, next) => next(null, el), Object.assign({ concurrency: 4 }, PERF_OPTIONS));
    const _duration = Date.now() - _start;
    assert.strictEqual(results.length, 20000);
    assert.deepStrictEqual(errors, []);
    assert.ok(_duration < 3000, `took ${_duration}ms, expected < 3000ms`);
  });

  it('should process 100 000 elements on 2 large chunks in less than 10 seconds (O(n²) shift protection)', async () => {
    const _list = Array.from({ length: 100000 }, (_, i) => i);
    const _start = Date.now();
    const { results } = await execQueueP('perf-large-chunk', _list, (el, next) => next(null, el), Object.assign({ concurrency: 2 }, PERF_OPTIONS));
    const _duration = Date.now() - _start;
    assert.strictEqual(results.length, 100000);
    assert.ok(_duration < 10000, `took ${_duration}ms, expected < 10000ms`);
  });

  it('should process 200 000 synchronous elements on a single queue in less than 5 seconds (batched executions)', async () => {
    const _list = Array.from({ length: 200000 }, (_, i) => i);
    const _start = Date.now();
    const { results } = await execQueueP('perf-batch', _list, (el, next) => next(null, el), Object.assign({ concurrency: 1 }, PERF_OPTIONS));
    const _duration = Date.now() - _start;
    assert.strictEqual(results.length, 200000);
    assert.ok(_duration < 5000, `took ${_duration}ms, expected < 5000ms (~${(_duration / 200).toFixed(1)}µs/element)`);
  });

  it('should handle 100 queues over 10 000 elements in less than 2 seconds (high concurrency)', async () => {
    const _list = Array.from({ length: 10000 }, (_, i) => i);
    const _start = Date.now();
    const { results, errors } = await execQueueP('perf-concurrency', _list, (el, next) => setImmediate(() => next(null, el)), Object.assign({ concurrency: 100 }, PERF_OPTIONS));
    const _duration = Date.now() - _start;
    assert.strictEqual(results.length, 10000);
    assert.deepStrictEqual(errors, []);
    assert.ok(_duration < 2000, `took ${_duration}ms, expected < 2000ms`);
  });

  it('should stay linear across 5 retry rounds of 10 000 failing elements in less than 5 seconds', async () => {
    const _list = Array.from({ length: 10000 }, (_, i) => i);
    let _executions = 0;
    const _start = Date.now();
    const { results, errors } = await execQueueP('perf-retry', _list, (el, next) => {
      _executions += 1;
      next(new Error('always fails'));
    }, Object.assign({}, PERF_OPTIONS, { concurrency: 4, retry: 5 }));
    const _duration = Date.now() - _start;
    /** 1 initial round + 5 retries, every element fails each time */
    assert.strictEqual(_executions, 60000);
    assert.strictEqual(results.length, 0);
    assert.strictEqual(errors.length, 10000);
    assert.ok(_duration < 5000, `took ${_duration}ms, expected < 5000ms`);
  });

  it('should keep the status rendering cheap when logQueueStatus is enabled without a TTY', () => {
    const { spawnSync } = require('child_process');
    const _script = `
      const { execQueue, setLogFunction } = require(${JSON.stringify(path.join(__dirname, '..', 'cqueue.js'))});
      let _lines = 0;
      setLogFunction((msg) => { _lines += String(msg).split('\\n').length; });
      const list = Array.from({ length: 5000 }, (_, i) => i);
      execQueue('render-bench', list, (el, next) => next(null, el), { concurrency: 2, logEnabled: false, logQueueStatus: true, retry: 0 }, () => process.stdout.write('LINES=' + _lines));
    `;
    const _start = Date.now();
    const _res = spawnSync(process.execPath, ['-e', _script], { encoding: 'utf8', timeout: 30000 });
    const _duration = Date.now() - _start;
    assert.strictEqual(_res.status, 0, _res.stderr);
    /** Nothing else on stdout: without a TTY the status goes through the configured log function */
    assert.match(_res.stdout, /^LINES=\d+$/, `unexpected output: ${_res.stdout.slice(0, 200)}`);
    /** Piped stdout = no TTY: the status must be printed on percentage steps, not once per element */
    const _lines = Number(_res.stdout.slice('LINES='.length));
    assert.ok(_lines <= 60, `expected at most 60 status lines, got ${_lines}`);
    assert.ok(_duration < 5000, `took ${_duration}ms, expected < 5000ms`);
  });

  it('should keep correct results and stats at scale with errors mixed in', async () => {
    const _list = Array.from({ length: 10000 }, (_, i) => i);
    const _start = Date.now();
    /** 1 element out of 100 fails: errors handling must not degrade the throughput */
    const { results, errors } = await execQueueP('perf-errors', _list, (el, next) => {
      return el % 100 === 0 ? next(new Error('planned failure')) : next(null, el);
    }, Object.assign({ concurrency: 4 }, PERF_OPTIONS));
    const _duration = Date.now() - _start;
    assert.strictEqual(results.length, 9900);
    assert.strictEqual(errors.length, 100);
    assert.ok(_duration < 3000, `took ${_duration}ms, expected < 3000ms`);
  });
});
