const { describe, it, before, beforeEach } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { msToTime, execQueue, chunkify, setLogFunction, NS_PER_SEC, MS_PER_NS } = require('../cqueue.js');

/**
 * All log output is captured in memory:
 * - keeps the test output clean
 * - allows assertions on logged messages
 */
let _capturedLogs = [];
/** Same messages with the level received by the log function, to assert on the levels */
let _capturedEntries = [];
setLogFunction((msg, level = 'info') => {
  _capturedLogs.push(String(msg));
  _capturedEntries.push({ msg: String(msg), level: level });
});

/** Level received by the log function for the first captured message containing `part` */
function levelOf (part) {
  return _capturedEntries.find(e => e.msg.includes(part))?.level;
}

/** Base options: silence the live queue status (which writes to stdout) */
function silentOptions (extra) {
  return Object.assign({ logEnabled: false, logQueueStatus: false }, extra);
}

/** Log files are written with a fire-and-forget fs.writeFile: poll until the file appears */
async function waitForLogFile (namePart, timeoutMs = 2000) {
  const _dir = path.join(process.cwd(), 'logs');
  const _deadline = Date.now() + timeoutMs;
  while (Date.now() < _deadline) {
    const _files = fs.existsSync(_dir) ? fs.readdirSync(_dir) : [];
    const _found = _files.find(f => f.includes(namePart));
    /** The file can exist while its content is not flushed yet: wait for a non-empty file */
    if (_found && fs.statSync(path.join(_dir, _found)).size > 0) {
      return path.join(_dir, _found);
    }
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  return null;
}

/** Promise wrapper to keep tests flat */
function execQueueP (queueName, list, functionToExecute, options) {
  return new Promise((resolve, reject) => {
    execQueue(queueName, list, functionToExecute, options, (err, results, errors) => {
      if (err) {
        return reject(err);
      }
      return resolve({ results, errors });
    });
  });
}

before(() => {
  /** Log files are written to `<cwd>/logs`: run the whole suite from a temp folder */
  const _tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cqueue-test-'));
  process.chdir(_tmp);
});

beforeEach(() => {
  _capturedLogs = [];
  _capturedEntries = [];
});

describe('constants', () => {
  it('should export time conversion constants', () => {
    assert.strictEqual(NS_PER_SEC, 1e9);
    assert.strictEqual(MS_PER_NS, 1e-6);
  });
});

describe('msToTime', () => {
  it('should format milliseconds', () => {
    assert.strictEqual(msToTime(0), '0 ms');
    assert.strictEqual(msToTime(500), '500 ms');
    assert.strictEqual(msToTime(949), '949 ms');
    /** 999ms must not be rounded up to "1.0 Sec" */
    assert.strictEqual(msToTime(999), '999 ms');
  });
  it('should format seconds', () => {
    assert.strictEqual(msToTime(1000), '1.0 Sec');
    assert.strictEqual(msToTime(10000), '10.0 Sec');
    assert.strictEqual(msToTime(59000), '59.0 Sec');
  });
  it('should format minutes', () => {
    assert.strictEqual(msToTime(60000), '1.0 Min');
    assert.strictEqual(msToTime(90000), '1.5 Min');
  });
  it('should format hours', () => {
    assert.strictEqual(msToTime(3600000), '1.0 Hrs');
    assert.strictEqual(msToTime(7200000), '2.0 Hrs');
  });
  it('should format days', () => {
    assert.strictEqual(msToTime(86400000), '1.0 Days');
    assert.strictEqual(msToTime(172800000), '2.0 Days');
  });
});

describe('chunkify', () => {
  it('should split a list into balanced chunks', () => {
    const _chunks = chunkify([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3, false);
    assert.strictEqual(_chunks.length, 3);
    assert.deepStrictEqual(_chunks.map(c => c.list.length), [4, 3, 3]);
    assert.deepStrictEqual(_chunks.map(c => c.id), [0, 1, 2]);
    assert.deepStrictEqual(_chunks.map(c => c.listLength), [4, 3, 3]);
    /** No element lost, order preserved */
    assert.deepStrictEqual(_chunks.flatMap(c => c.list), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  });
  it('should return a single chunk when size is 1', () => {
    const _chunks = chunkify(['a', 'b', 'c'], 1, false);
    assert.strictEqual(_chunks.length, 1);
    assert.deepStrictEqual(_chunks[0].list, ['a', 'b', 'c']);
  });
  it('should create empty chunks when size is greater than the list length', () => {
    const _chunks = chunkify(['a', 'b'], 5, false);
    assert.strictEqual(_chunks.length, 5);
    assert.deepStrictEqual(_chunks.flatMap(c => c.list), ['a', 'b']);
  });
  it('should not mutate the source list', () => {
    const _source = [1, 2, 3];
    chunkify(_source, 2, false);
    assert.deepStrictEqual(_source, [1, 2, 3]);
  });
  it('should initialise queue state on each chunk', () => {
    const _chunk = chunkify([1], 1, false)[0];
    assert.strictEqual(_chunk.done, false);
    assert.strictEqual(_chunk.cursor, 0);
    assert.deepStrictEqual(_chunk.errors, []);
    assert.deepStrictEqual(_chunk.results, []);
    assert.deepStrictEqual(_chunk.logs, []);
    assert.deepStrictEqual(_chunk.time, { requestTime: 0, averageTime: 0, leftTime: 0, passedTime: 0, percentage: 0, done: 0 });
  });
});

describe('execQueue', () => {
  it('should process every element and return results in order (concurrency 1)', async () => {
    const _processed = [];
    const { results, errors } = await execQueueP('basic', [1, 2, 3, 4, 5], (el, next) => {
      _processed.push(el);
      next(null, el * 2);
    }, silentOptions());
    assert.deepStrictEqual(_processed, [1, 2, 3, 4, 5]);
    assert.deepStrictEqual(results, [2, 4, 6, 8, 10]);
    assert.deepStrictEqual(errors, []);
  });

  it('should work with the 4 arguments form (no options)', (t, done) => {
    execQueue('no-options', ['a', 'b'], (el, next) => next(null, el.toUpperCase()), (err, results) => {
      assert.strictEqual(err, null);
      assert.deepStrictEqual(results.sort(), ['A', 'B']);
      /** Default options enable the start/end summary logs */
      assert.ok(_capturedLogs.some(l => l.includes('[no-options] START - 2 total elements')));
      assert.ok(_capturedLogs.some(l => l.includes('[no-options] END')));
      done();
    });
  });

  it('should handle an empty list', async () => {
    const { results, errors } = await execQueueP('empty', [], (el, next) => next(null, el), silentOptions());
    assert.deepStrictEqual(results, []);
    assert.deepStrictEqual(errors, []);
  });

  it('should not include results when the executed function returns nothing', async () => {
    const { results } = await execQueueP('void', [1, 2, 3], (el, next) => next(), silentOptions());
    assert.deepStrictEqual(results, []);
  });

  it('should process all elements with multiple queues and run them in parallel', async () => {
    let _inFlight = 0;
    let _maxInFlight = 0;
    const _list = Array.from({ length: 9 }, (_, i) => i);
    const { results } = await execQueueP('parallel', _list, (el, next) => {
      _inFlight += 1;
      _maxInFlight = Math.max(_maxInFlight, _inFlight);
      setTimeout(() => {
        _inFlight -= 1;
        next(null, el);
      }, 10);
    }, silentOptions({ concurrency: 3 }));
    assert.strictEqual(results.length, 9);
    assert.deepStrictEqual([...results].sort((a, b) => a - b), _list);
    assert.ok(_maxInFlight >= 2, `expected at least 2 executions in parallel, got ${_maxInFlight}`);
  });

  it('should wait `delay` ms between executions', async () => {
    const _start = Date.now();
    await execQueueP('delay', [1, 2, 3, 4], (el, next) => next(null, el), silentOptions({ delay: 25 }));
    const _duration = Date.now() - _start;
    assert.ok(_duration >= 75, `expected at least 75ms, got ${_duration}ms`);
  });

  describe('errors and retries', () => {
    it('should retry failed elements and return remaining errors as third callback argument', async () => {
      const _attempts = {};
      const { results, errors } = await execQueueP('always-fails', ['ok1', 'bad', 'ok2'], (el, next) => {
        _attempts[el] = (_attempts[el] ?? 0) + 1;
        if (el === 'bad') {
          return next(new Error('boom'));
        }
        return next(null, el);
      }, silentOptions());
      /** default retry = 1: the failed element is executed twice, successes only once */
      assert.deepStrictEqual(_attempts, { ok1: 1, bad: 2, ok2: 1 });
      assert.deepStrictEqual([...results].sort(), ['ok1', 'ok2']);
      assert.strictEqual(errors.length, 1);
      assert.strictEqual(errors[0].element, 'bad');
      assert.match(errors[0].message, /boom/);
    });

    it('should succeed when the element succeeds during a retry', async () => {
      let _tries = 0;
      const { results, errors } = await execQueueP('transient', ['flaky'], (el, next) => {
        _tries += 1;
        if (_tries === 1) {
          return next(new Error('transient failure'));
        }
        return next(null, 'recovered');
      }, silentOptions());
      assert.deepStrictEqual(results, ['recovered']);
      assert.deepStrictEqual(errors, []);
    });

    it('should not retry when retry is 0', async () => {
      let _tries = 0;
      const { errors } = await execQueueP('no-retry', ['bad'], (el, next) => {
        _tries += 1;
        next(new Error('nope'));
      }, silentOptions({ retry: 0 }));
      assert.strictEqual(_tries, 1);
      assert.strictEqual(errors.length, 1);
    });

    it('should retry `retry` times', async () => {
      let _tries = 0;
      const { errors } = await execQueueP('retry-3', ['bad'], (el, next) => {
        _tries += 1;
        next(new Error('nope'));
      }, silentOptions({ retry: 3 }));
      assert.strictEqual(_tries, 4);
      assert.strictEqual(errors.length, 1);
    });

    it('should record an error when the executed function throws synchronously', async () => {
      const { results, errors } = await execQueueP('sync-throw', ['ok', 'boom'], (el, next) => {
        if (el === 'boom') {
          throw new Error('sync explosion');
        }
        next(null, el);
      }, silentOptions({ retry: 0 }));
      assert.deepStrictEqual(results, ['ok']);
      assert.strictEqual(errors.length, 1);
      assert.match(errors[0].message, /sync explosion/);
    });

    it('should create an error log file on failure', async () => {
      await execQueueP('My Error Queue', ['bad'], (el, next) => next(new Error('nope')), silentOptions({ retry: 0 }));
      const _file = await waitForLogFile('my-error-queue-errors');
      assert.ok(_file, 'error file not found');
      const _content = JSON.parse(fs.readFileSync(_file, 'utf8'));
      assert.strictEqual(_content.length, 1);
      assert.strictEqual(_content[0].element, 'bad');
    });

    it('should create one error file per retry attempt instead of overwriting the same file', async () => {
      await execQueueP('Retry Files', ['bad'], (el, next) => next(new Error('nope')), silentOptions({ retry: 1 }));
      /** attempt 0 has no suffix, attempt 1 is suffixed with -try1 */
      const _first = await waitForLogFile('retry-files-errors.json');
      const _second = await waitForLogFile('retry-files-errors-try1.json');
      assert.ok(_first, 'error file of the first attempt not found');
      assert.ok(_second, 'error file of the retry attempt not found');
    });

    it('should record both the error and the result when the callback provides both', async () => {
      const { results, errors } = await execQueueP('err-and-res', ['x'], (el, next) => {
        next(new Error('partial'), 'still-a-result');
      }, silentOptions({ retry: 0 }));
      assert.deepStrictEqual(results, ['still-a-result']);
      assert.strictEqual(errors.length, 1);
    });
  });

  describe('actions', () => {
    it('should stop the queue when the action stop is true', async () => {
      const _processed = [];
      const { results, errors } = await execQueueP('stop', [1, 2, 3, 4, 5], (el, next) => {
        _processed.push(el);
        next(null, el, { stop: el === 2 });
      }, silentOptions());
      assert.deepStrictEqual(_processed, [1, 2]);
      assert.deepStrictEqual(results, [1, 2]);
      assert.deepStrictEqual(errors, []);
    });

    it('should stop ALL queues when one queue receives the stop action', async () => {
      const _processed = [];
      const _list = Array.from({ length: 30 }, (_, i) => i);
      const { results } = await execQueueP('stop-all', _list, (el, next) => {
        _processed.push(el);
        setTimeout(() => next(null, el, { stop: el === 0 }), 5);
      }, silentOptions({ concurrency: 3 }));
      /** Element 0 is the first element of queue 0: the two other queues must stop after their in-flight element */
      assert.ok(_processed.length <= 6, `expected at most 6 processed elements, got ${_processed.length}`);
      assert.ok(results.length <= 6);
      assert.ok(_processed.length < _list.length, 'stop did not prevent the full list from being processed');
    });

    it('should collect logs provided through actions and write a log file', async () => {
      await execQueueP('My Log Queue', [1, 2], (el, next) => {
        next(null, el, { logs: [`log-${el}`] });
      }, silentOptions());
      const _logFile = await waitForLogFile('my-log-queue-logs');
      assert.ok(_logFile, 'log file not found');
      const _content = JSON.parse(fs.readFileSync(_logFile, 'utf8'));
      assert.deepStrictEqual([..._content].sort(), ['log-1', 'log-2']);
    });

    it('should serialize non-JSON-serializable log entries as null', async () => {
      await execQueueP('Weird Logs', [1], (el, next) => {
        next(null, el, { logs: ['valid', undefined, () => {}] });
      }, silentOptions());
      const _logFile = await waitForLogFile('weird-logs-logs');
      const _content = JSON.parse(fs.readFileSync(_logFile, 'utf8'));
      assert.deepStrictEqual(_content, ['valid', null, null]);
    });

    it('should write a large log file as valid JSON (chunked serialization)', async () => {
      const _entries = Array.from({ length: 5000 }, (_, i) => ({ id: i, msg: `entry-${i}` }));
      await execQueueP('Big Logs', [1], (el, next) => {
        next(null, el, { logs: _entries });
      }, silentOptions());
      const _logFile = await waitForLogFile('big-logs-logs');
      /** Poll until the streamed file is complete and parses */
      let _content = null;
      for (let i = 0; i < 100 && _content === null; i++) {
        try {
          _content = JSON.parse(fs.readFileSync(_logFile, 'utf8'));
        } catch (e) {
          await new Promise(resolve => setTimeout(resolve, 20));
        }
      }
      assert.ok(_content, 'log file never became valid JSON');
      assert.strictEqual(_content.length, 5000);
      assert.deepStrictEqual(_content[4999], { id: 4999, msg: 'entry-4999' });
    });

    it('should ignore a logs action which is not an array', async () => {
      const { results } = await execQueueP('bad-logs', [1], (el, next) => {
        next(null, el, { logs: 'not-an-array' });
      }, silentOptions());
      assert.deepStrictEqual(results, [1]);
    });
  });

  describe('robustness', () => {
    it('should process an element only once when the callback is called twice', async () => {
      const { results } = await execQueueP('double-callback', [1, 2, 3], (el, next) => {
        next(null, el);
        next(null, el);
      }, silentOptions());
      assert.deepStrictEqual(results, [1, 2, 3]);
    });

    it('should keep falsy results such as 0, empty string and false', async () => {
      const { results } = await execQueueP('falsy', [0, '', false, null, undefined], (el, next) => next(null, el), silentOptions());
      assert.deepStrictEqual(results, [0, '', false]);
    });

    it('should not mutate the options object provided by the caller', async () => {
      const _options = { concurrency: 2, retry: 0, logEnabled: false, logQueueStatus: false };
      const _snapshot = JSON.parse(JSON.stringify(_options));
      await execQueueP('no-mutation', [1, 2], (el, next) => next(null, el), _options);
      assert.deepStrictEqual(_options, _snapshot);
    });

    it('should keep the event loop responsive during a long synchronous batch', async () => {
      let _ticks = 0;
      const _interval = setInterval(() => { _ticks += 1; }, 10);
      const _list = Array.from({ length: 2000 }, (_, i) => i);
      const { results } = await execQueueP('fairness', _list, (el, next) => {
        /** Burn ~50µs of synchronous CPU per element (~100ms total) */
        const _until = process.hrtime.bigint() + 50000n;
        while (process.hrtime.bigint() < _until) { /* busy */ }
        next(null, el);
      }, silentOptions());
      clearInterval(_interval);
      assert.strictEqual(results.length, 2000);
      /** The synchronous batches must yield back to the event loop so timers keep firing */
      assert.ok(_ticks >= 2, `expected the 10ms interval to fire at least twice during the run, got ${_ticks}`);
    });

    it('should not leak results when the same options object is reused between two runs', async () => {
      const _options = silentOptions();
      const _first = await execQueueP('reuse-1', [1], (el, next) => next(null, el), _options);
      assert.deepStrictEqual(_first.results, [1]);
      const _second = await execQueueP('reuse-2', [2], (el, next) => next(null, el), _options);
      assert.deepStrictEqual(_second.results, [2]);
    });
  });

  describe('promise mode (no callback)', () => {
    it('should resolve with { results, errors } when options are provided without a callback', async () => {
      const { results, errors } = await execQueue('promise-options', [1, 2, 3], (el, next) => next(null, el * 10), silentOptions());
      assert.deepStrictEqual(results, [10, 20, 30]);
      assert.deepStrictEqual(errors, []);
    });

    it('should resolve without options nor callback', async () => {
      const { results, errors } = await execQueue('promise-bare', [1], (el, next) => next(null, el));
      assert.deepStrictEqual(results, [1]);
      assert.deepStrictEqual(errors, []);
    });

    it('should resolve with the remaining errors after the retries', async () => {
      const { results, errors } = await execQueue('promise-errors', ['bad'], (el, next) => next(new Error('nope')), silentOptions({ retry: 0 }));
      assert.deepStrictEqual(results, []);
      assert.strictEqual(errors.length, 1);
      assert.strictEqual(errors[0].element, 'bad');
    });
  });

  describe('logging', () => {
    it('should log START and END summaries when logEnabled is true', async () => {
      await execQueueP('log-run', [1, 2], (el, next) => next(null, el), { logEnabled: true, logQueueStatus: false });
      assert.ok(_capturedLogs.some(l => l.includes('[log-run] START - 2 total elements - 1 queue(s)')));
      assert.ok(_capturedLogs.some(l => l.includes('[log-run] END - Duration:')));
    });

    it('should not log START/END when logEnabled is false', async () => {
      await execQueueP('quiet-run', [1], (el, next) => next(null, el), silentOptions());
      assert.strictEqual(_capturedLogs.length, 0);
    });

    it('should use the function provided to setLogFunction', async () => {
      /** The suite-wide capture function proves setLogFunction works; verify explicitly */
      await execQueueP('custom-log', [1], (el, next) => next(new Error('log me')), silentOptions({ retry: 0 }));
      assert.ok(_capturedLogs.some(l => l.includes('[custom-log] 1 errors')));
    });

    it('should keep the default console output working when no log function is registered', async () => {
      /** A fresh module instance: its output is the default console.log, never replaced */
      delete require.cache[require.resolve('../cqueue.js')];
      const _fresh = require('../cqueue.js');
      const _written = [];
      const _consoleLog = console.log;
      console.log = (msg) => _written.push(String(msg));
      try {
        await new Promise(resolve => _fresh.execQueue('default-output', [1], (el, next) => next(new Error('boom')), silentOptions({ retry: 0 }), () => resolve()));
      } finally {
        console.log = _consoleLog;
        delete require.cache[require.resolve('../cqueue.js')];
      }
      /** The default output marks the error level and keeps the message otherwise unchanged */
      assert.ok(_written.some(l => l === 'ERROR [default-output] 1 errors: "Error: boom" x1'), _written.join('\n'));
      assert.ok(_written.some(l => l.startsWith('ERROR [default-output] Created errors file:')), _written.join('\n'));
    });
  });

  describe('log levels', () => {
    it('should log the error paths at the error level', async () => {
      await execQueueP('level-run', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0 }));
      assert.strictEqual(levelOf('] 1 errors'), 'error');
      /** The error file path must not be stranded at info while the errors are logged at error */
      assert.strictEqual(levelOf('Created errors file:'), 'error');
      assert.strictEqual(levelOf('END - Stop retrying, check the error file!'), 'error');
    });

    it('should log a retry about to be attempted at the warn level', async () => {
      let _calls = 0;
      await execQueueP('level-retry', ['a'], (el, next) => next(++_calls === 1 ? new Error('boom') : null, el), silentOptions({ retry: 1 }));
      assert.strictEqual(levelOf('Retry to re-execute the process on failled elements'), 'warn');
    });

    it('should log the log file path at the info level', async () => {
      await execQueueP('level-logs', ['a'], (el, next) => next(null, el, { logs: ['hello'] }), silentOptions());
      assert.strictEqual(levelOf('Created logs file:'), 'info');
    });

    it('should log a caught Promise All error at the error level', async () => {
      /** A non-string queue name makes the error file name generation throw inside the try block */
      await assert.rejects(execQueueP(123, ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0 })));
      assert.strictEqual(levelOf('Error: Promise All Catched:'), 'error');
    });

    it('should log a log folder creation error at the error level', async () => {
      const _cwd = process.cwd();
      const _tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cqueue-nofolder-'));
      /** A file named `logs` makes the mkdir of the logs folder fail */
      fs.writeFileSync(path.join(_tmp, 'logs'), '');
      process.chdir(_tmp);
      try {
        await execQueueP('level-folder', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0 }));
      } finally {
        process.chdir(_cwd);
      }
      assert.strictEqual(levelOf('Error Create Log Folder:'), 'error');
    });

    it('should log a log file creation error at the error level', async (t) => {
      if (process.getuid && process.getuid() === 0) {
        return t.skip('root bypasses the folder permissions');
      }
      const _cwd = process.cwd();
      const _tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cqueue-nowrite-'));
      const _logs = path.join(_tmp, 'logs');
      /** A read-only logs folder: the mkdir succeeds, the write stream fails */
      fs.mkdirSync(_logs);
      fs.chmodSync(_logs, 0o500);
      process.chdir(_tmp);
      try {
        await execQueueP('level-file', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0 }));
        /** The stream error event is emitted asynchronously */
        await new Promise(resolve => setTimeout(resolve, 100));
      } finally {
        process.chdir(_cwd);
        fs.chmodSync(_logs, 0o700);
      }
      assert.strictEqual(levelOf('Error Create Log File:'), 'error');
    });
  });

  describe('error summary', () => {
    it('should summarize the distinct error messages with their count, most frequent first', async () => {
      await execQueueP('summary', ['boom', 'nope', 'boom', 'boom'], (el, next) => next(new Error(el)), silentOptions({ retry: 0 }));
      const _line = _capturedLogs.find(l => l.includes('] 4 errors'));
      assert.strictEqual(_line, '[summary] 4 errors: "Error: boom" x3, "Error: nope" x1');
    });

    it('should cap the summary and count the distinct messages left out', async () => {
      await execQueueP('summary-cap', ['e1', 'e1', 'e2', 'e2', 'e3', 'e4', 'e5'], (el, next) => next(new Error(el)), silentOptions({ retry: 0 }));
      const _line = _capturedLogs.find(l => l.includes('] 7 errors'));
      /** Top 3 distinct messages, equal counts keep their first-seen order, then "+N more" */
      assert.strictEqual(_line, '[summary-cap] 7 errors: "Error: e1" x2, "Error: e2" x2, "Error: e3" x1, +2 more');
    });

    it('should not cap the summary when the distinct messages fit', async () => {
      await execQueueP('summary-fit', ['e1', 'e2', 'e3'], (el, next) => next(new Error(el)), silentOptions({ retry: 0 }));
      const _line = _capturedLogs.find(l => l.includes('] 3 errors'));
      assert.strictEqual(_line, '[summary-fit] 3 errors: "Error: e1" x1, "Error: e2" x1, "Error: e3" x1');
      assert.ok(!_line.includes('more'));
    });

    it('should truncate a long error message in the summary', async () => {
      const _long = 'x'.repeat(200);
      await execQueueP('summary-long', [_long], (el, next) => next(new Error(el)), silentOptions({ retry: 0 }));
      const _line = _capturedLogs.find(l => l.includes('] 1 errors'));
      /** 100 characters max per message: 99 kept plus the ellipsis */
      assert.strictEqual(_line, `[summary-long] 1 errors: "${('Error: ' + _long).slice(0, 99)}…" x1`);
    });

    it('should not truncate a message sitting exactly on the limit', async () => {
      /** 100 characters with the "Error: " prefix included */
      const _exact = 'x'.repeat(93);
      await execQueueP('summary-exact', [_exact], (el, next) => next(new Error(el)), silentOptions({ retry: 0 }));
      const _line = _capturedLogs.find(l => l.includes('] 1 errors'));
      assert.strictEqual(_line, `[summary-exact] 1 errors: "Error: ${_exact}" x1`);
    });

    it('should label an empty error message as unknown', async () => {
      await execQueueP('summary-empty', ['a'], (el, next) => next({ toString: () => '' }), silentOptions({ retry: 0 }));
      const _line = _capturedLogs.find(l => l.includes('] 1 errors'));
      assert.strictEqual(_line, '[summary-empty] 1 errors: "Unknown error" x1');
    });

    it('should keep writing the full error file next to the summary', async () => {
      await execQueueP('Summary File', ['boom', 'boom'], (el, next) => next(new Error(el)), silentOptions({ retry: 0 }));
      const _file = await waitForLogFile('summary-file-errors');
      assert.ok(_file, 'error file not found');
      const _content = JSON.parse(fs.readFileSync(_file, 'utf8'));
      /** Unchanged format: one entry per error, not the deduplicated summary */
      assert.deepStrictEqual(_content, [
        { element: 'boom', message: 'Error: boom' },
        { element: 'boom', message: 'Error: boom' }
      ]);
    });
  });
});
