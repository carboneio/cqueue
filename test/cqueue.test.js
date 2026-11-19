const { describe, it, before, beforeEach } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { msToTime, execQueue, chunkify, setLogFunction, setDefaultOptions, NS_PER_SEC, MS_PER_NS } = require('../cqueue.js');

/**
 * All log output is captured in memory:
 * - keeps the test output clean
 * - allows assertions on logged messages
 */
let _capturedLogs = [];
/** Same messages with the level received by the log function, to assert on the levels */
let _capturedEntries = [];
setLogFunction((msg, level = 'info', fields) => {
  _capturedLogs.push(String(msg));
  _capturedEntries.push({ msg: String(msg), level: level, fields: fields });
});

/** Level received by the log function for the first captured message containing `part` */
function levelOf (part) {
  return _capturedEntries.find(e => e.msg.includes(part))?.level;
}

/** Captured entries of a structured event, in logging order */
function entriesOf (event) {
  return _capturedEntries.filter(e => e.fields?.event === event);
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

const CQUEUE_PATH = path.join(__dirname, '..', 'cqueue.js');

/** Make execQueue fail internally: Promise.allSettled is its first call inside its try block,
 * made synchronously, so the original is restored before anything else can use it */
function withInternalError (fn, message = 'internal failure') {
  const _allSettled = Promise.allSettled;
  Promise.allSettled = () => Promise.reject(new TypeError(message));
  try {
    return fn();
  } finally {
    Promise.allSettled = _allSettled;
  }
}

/** A dedicated empty folder, used as the `logDir` option */
function tmpLogDir () {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'cqueue-logdir-'));
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
  /** Global options are module state: never leak them from one test to the next */
  setDefaultOptions(null);
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

  describe('log file completion', () => {
    it('should have fully written the error file when the callback is called', async () => {
      const _dir = tmpLogDir();
      const _list = Array.from({ length: 5000 }, (_, i) => `bad-${i}`);
      await execQueueP('complete-errors', _list, (el, next) => next(new Error(el)), silentOptions({ retry: 0, logDir: _dir }));
      /** No polling: the file must already be complete and parseable when the queue calls back */
      const _file = fs.readdirSync(_dir).find(f => f.includes('complete-errors-errors'));
      assert.ok(_file, 'error file not found');
      const _content = JSON.parse(fs.readFileSync(path.join(_dir, _file), 'utf8'));
      assert.strictEqual(_content.length, 5000);
    });

    it('should have fully written the logs file when the callback is called', async () => {
      const _dir = tmpLogDir();
      const _entries = Array.from({ length: 5000 }, (_, i) => ({ id: i }));
      await execQueueP('complete-logs', [1], (el, next) => next(null, el, { logs: _entries }), silentOptions({ logDir: _dir }));
      const _file = fs.readdirSync(_dir).find(f => f.includes('complete-logs-logs'));
      assert.ok(_file, 'log file not found');
      const _content = JSON.parse(fs.readFileSync(path.join(_dir, _file), 'utf8'));
      assert.strictEqual(_content.length, 5000);
    });

    it('should write a complete error file when the caller exits from the callback', () => {
      const { spawnSync } = require('child_process');
      const _dir = tmpLogDir();
      /** The regression: a CLI exiting from the callback used to truncate the chunked write */
      const _script = `
        const { execQueue, setLogFunction } = require(${JSON.stringify(CQUEUE_PATH)});
        setLogFunction(() => {});
        const list = Array.from({ length: 5000 }, (_, i) => 'bad-' + i);
        execQueue('exit-queue', list, (el, next) => next(new Error(el)), { logEnabled: false, logQueueStatus: false, retry: 0, logDir: ${JSON.stringify(_dir)} }, () => process.exit(0));
      `;
      const _res = spawnSync(process.execPath, ['-e', _script], { encoding: 'utf8', timeout: 30000 });
      assert.strictEqual(_res.status, 0, _res.stderr);
      const _file = fs.readdirSync(_dir).find(f => f.includes('exit-queue-errors'));
      assert.ok(_file, 'error file not found');
      const _content = JSON.parse(fs.readFileSync(path.join(_dir, _file), 'utf8'));
      assert.strictEqual(_content.length, 5000);
    });

    it('should return the results and the errors when the file cannot be written', async (t) => {
      if (process.getuid && process.getuid() === 0) {
        return t.skip('root bypasses the folder permissions');
      }
      const _dir = tmpLogDir();
      fs.chmodSync(_dir, 0o500);
      try {
        const { results, errors } = await execQueueP('nowrite', ['ok', 'bad'], (el, next) => {
          return el === 'bad' ? next(new Error('boom')) : next(null, el);
        }, silentOptions({ retry: 0, logDir: _dir }));
        /** A write failure must not swallow the results, nor hang the queue */
        assert.deepStrictEqual(results, ['ok']);
        assert.strictEqual(errors.length, 1);
      } finally {
        fs.chmodSync(_dir, 0o700);
      }
      assert.strictEqual(levelOf('Error Create Log File:'), 'error');
    });
  });

  describe('logDir option', () => {
    it('should write the files in the given directory', async () => {
      const _dir = tmpLogDir();
      await execQueueP('custom-dir', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir }));
      assert.ok(fs.readdirSync(_dir).some(f => f.includes('custom-dir-errors')));
      /** The default `<cwd>/logs` folder is not used any more for this queue */
      assert.ok(!fs.readdirSync(path.join(process.cwd(), 'logs')).some(f => f.includes('custom-dir-errors')));
    });

    it('should create the directory when it does not exist', async () => {
      const _dir = path.join(tmpLogDir(), 'nested', 'queue-logs');
      await execQueueP('nested-dir', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir }));
      assert.ok(fs.readdirSync(_dir).some(f => f.includes('nested-dir-errors')));
    });

    it('should log the path of the given directory', async () => {
      const _dir = tmpLogDir();
      await execQueueP('logged-dir', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir }));
      assert.ok(_capturedLogs.some(l => l.includes(`Created errors file: ${path.join(_dir, '')}`)));
    });
  });

  describe('logRetentionDays option', () => {
    /** Write a file named like a log file of `queueName`, `days` days ago */
    function writeAgedFile (dir, queueName, label, days) {
      const _stamp = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString().replace(/:/g, '-').slice(0, 19);
      const _name = `${_stamp}-${queueName}${label ? '-' + label : ''}.json`;
      fs.writeFileSync(path.join(dir, _name), '[]');
      return _name;
    }

    it('should keep every file by default', async () => {
      const _dir = tmpLogDir();
      const _old = writeAgedFile(_dir, 'kept-all', 'errors', 400);
      await execQueueP('kept-all', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir }));
      assert.ok(fs.existsSync(path.join(_dir, _old)), 'a 400 days old file was deleted without the option');
    });

    it('should delete the files older than the given number of days', async () => {
      const _dir = tmpLogDir();
      const _old = writeAgedFile(_dir, 'aged', 'errors', 30);
      const _recent = writeAgedFile(_dir, 'aged', 'errors', 3);
      await execQueueP('aged', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir, logRetentionDays: 7 }));
      assert.ok(!fs.existsSync(path.join(_dir, _old)), 'the 30 days old file was kept');
      /** Younger than the retention: kept, like the file of the current run */
      assert.ok(fs.existsSync(path.join(_dir, _recent)), 'the 3 days old file was deleted');
      assert.strictEqual(fs.readdirSync(_dir).filter(f => f.includes('aged-errors')).length, 2);
    });

    it('should accept a fraction of a day', async () => {
      const _dir = tmpLogDir();
      const _old = writeAgedFile(_dir, 'fraction', 'errors', 1 / 24);
      await execQueueP('fraction', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir, logRetentionDays: 1 / 48 }));
      assert.ok(!fs.existsSync(path.join(_dir, _old)), 'the one hour old file was kept');
    });

    it('should never delete the file of the current run', async () => {
      const _dir = tmpLogDir();
      /** A retention shorter than the second precision of the file name must not delete it */
      await execQueueP('current', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir, logRetentionDays: 1 / 86400000 }));
      assert.strictEqual(fs.readdirSync(_dir).filter(f => f.includes('current-errors')).length, 1);
    });

    it('should only delete the files of the same queue and label', async () => {
      const _dir = tmpLogDir();
      const _foreign = writeAgedFile(_dir, 'other-queue', 'errors', 30);
      const _otherLabel = writeAgedFile(_dir, 'isolated', 'logs', 30);
      const _unknown = path.join(_dir, 'not-a-log-file.json');
      fs.writeFileSync(_unknown, '[]');
      await execQueueP('isolated', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir, logRetentionDays: 7 }));
      assert.ok(fs.existsSync(path.join(_dir, _foreign)), 'the file of another queue was deleted');
      assert.ok(fs.existsSync(path.join(_dir, _otherLabel)), 'the logs file was deleted by the errors cleanup');
      assert.ok(fs.existsSync(_unknown), 'an unrelated file was deleted');
    });

    it('should not fail the queue when an old file cannot be deleted', async () => {
      const _dir = tmpLogDir();
      /** A directory named like an old log file: fs.unlink cannot remove it */
      const _stamp = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().replace(/:/g, '-').slice(0, 19);
      fs.mkdirSync(path.join(_dir, `${_stamp}-undeletable-errors.json`));
      const { errors } = await execQueueP('undeletable', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir, logRetentionDays: 7 }));
      /** The cleanup failure is a warning, the queue result is intact */
      assert.strictEqual(errors.length, 1);
      assert.strictEqual(levelOf('Error Remove Log File:'), 'warn');
    });

    it('should not fail the queue when the directory cannot be listed', async (t) => {
      if (process.getuid && process.getuid() === 0) {
        return t.skip('root bypasses the folder permissions');
      }
      const _dir = tmpLogDir();
      /** Write and execute but no read: the file is created, the cleanup listing fails */
      fs.chmodSync(_dir, 0o300);
      try {
        const { errors } = await execQueueP('unlistable', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir, logRetentionDays: 7 }));
        assert.strictEqual(errors.length, 1);
      } finally {
        fs.chmodSync(_dir, 0o700);
      }
      assert.strictEqual(levelOf('Error Read Log Folder:'), 'warn');
    });
  });

  describe('status rendering without a TTY', () => {
    it('should route the status block through the log function', async () => {
      await execQueueP('status-log', [1, 2], (el, next) => next(null, el), { logEnabled: false, logQueueStatus: true });
      assert.ok(_capturedLogs.some(l => l.includes('[0] 100% - 2/2')), _capturedLogs.join('|'));
      assert.strictEqual(levelOf('[0] 100% - 2/2'), 'info');
    });

    it('should keep printing the status every 10% of progress only', async () => {
      const _list = Array.from({ length: 100 }, (_, i) => i);
      await execQueueP('status-throttle', _list, (el, next) => next(null, el), { logEnabled: false, logQueueStatus: true });
      const _status = _capturedLogs.filter(l => l.startsWith('[0] '));
      /** One block per 10% step plus the final one, never one per element */
      assert.ok(_status.length <= 12 && _status.length >= 2, `got ${_status.length} status prints`);
    });

    it('should print one line per queue in a single block', async () => {
      await execQueueP('status-block', [1, 2, 3, 4], (el, next) => next(null, el), { concurrency: 2, logEnabled: false, logQueueStatus: true });
      const _block = _capturedLogs.find(l => l.includes('[0] 100%'));
      assert.ok(_block, _capturedLogs.join('|'));
      assert.strictEqual(_block.split('\n').length, 2);
      assert.ok(_block.includes('[1] '));
    });
  });

  describe('coverage of the remaining paths', () => {
    it('should apply the delay between asynchronous executions', async () => {
      const _start = Date.now();
      /** An asynchronous worker takes the delayed resume path, a synchronous one does not */
      await execQueueP('async-delay', [1, 2, 3], (el, next) => setTimeout(() => next(null, el), 1), silentOptions({ delay: 25 }));
      assert.ok(Date.now() - _start >= 50, `took ${Date.now() - _start}ms, expected at least 50ms`);
    });

    it('should append the error count to the status line', async () => {
      await execQueueP('status-errors', [1, 2], (el, next) => next(new Error('boom')), { logEnabled: false, logQueueStatus: true, retry: 0, logDir: tmpLogDir() });
      assert.ok(_capturedLogs.some(l => l.includes('[0] 100% - 2/2') && l.includes('2 errors')), _capturedLogs.join('|'));
    });

    it('should summarize the performances of the slowest queue', async () => {
      await execQueueP('perf-summary', [1, 2, 3, 4], (el, next) => next(null, el), { concurrency: 2, logEnabled: true, logQueueStatus: false });
      /** The summary picks the slowest of the queues, which needs more than one queue */
      assert.ok(_capturedLogs.some(l => /\[perf-summary\] END - Duration: .* \| Errors: 0 \| Returned: 4 \| Logs: 0/.test(l)), _capturedLogs.join('|'));
    });

    it('should reject the promise on an internal error when no callback is given', async () => {
      await assert.rejects(
        withInternalError(() => execQueue('internal', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0 }))),
        (err) => err instanceof Error && err.message.includes('Promise All Catched')
      );
    });
  });

  describe('TTY status rendering', () => {
    /** `isTTY` and `columns` are plain properties of the stream, so the terminal rendering is
     * exercised in a child process whose stdout is a pipe, without stubbing the runner's own output */
    function runInFakeTTY (body) {
      const { spawnSync } = require('child_process');
      const _script = `
        process.stdout.isTTY = true;
        process.stdout.columns = 80;
        const { execQueue, setLogFunction } = require(${JSON.stringify(CQUEUE_PATH)});
        setLogFunction((msg) => process.stderr.write(String(msg) + '\\n'));
        ${body}
      `;
      const _res = spawnSync(process.execPath, ['-e', _script], { encoding: 'utf8', timeout: 30000 });
      assert.strictEqual(_res.status, 0, _res.stderr);
      return _res;
    }

    it('should repaint the status block in place', () => {
      const _res = runInFakeTTY(`
        const list = Array.from({ length: 50 }, (_, i) => i);
        execQueue('tty', list, (el, next) => setTimeout(() => next(null, el), 1), { logEnabled: false, logQueueStatus: true }, () => {});
      `);
      /** Cursor hidden, block repainted above itself, end of each line cleared */
      assert.ok(_res.stdout.includes('\x1b[?25l'), 'the cursor was not hidden');
      assert.ok(/\x1b\[\d+A/.test(_res.stdout), 'the block was not repainted in place');
      assert.ok(_res.stdout.includes('\x1b[K'), 'the end of the line was not cleared');
      assert.ok(_res.stdout.includes('[0] 100%'), 'the final state was not painted');
      /** 80 columns: the status line is truncated instead of wrapping */
      assert.ok(_res.stdout.includes('…'), 'the line was not truncated to the terminal width');
    });

    it('should erase and repaint the block around a log message', () => {
      const _res = runInFakeTTY(`
        const list = Array.from({ length: 30 }, (_, i) => i);
        /** One queue paints its block while another one logs its START line above it */
        execQueue('tty-bg', list, (el, next) => setTimeout(() => next(null, el), 2), { logEnabled: false, logQueueStatus: true }, () => {});
        setTimeout(() => execQueue('tty-log', [1], (el, next) => next(null, el), { logEnabled: true, logQueueStatus: false }, () => {}), 10);
      `);
      assert.ok(_res.stdout.includes('\x1b[J'), 'the painted block was not erased before the log message');
      assert.ok(_res.stderr.includes('[tty-log] START'), 'the log message was lost');
    });
  });

  describe('setDefaultOptions', () => {
    it('should apply the global options when the call provides none', async () => {
      const _dir = tmpLogDir();
      setDefaultOptions({ concurrency: 3, retry: 0, logEnabled: false, logQueueStatus: false, logDir: _dir });
      const { results, errors } = await execQueue('global-opts', ['a', 'b'], (el, next) => next(new Error('boom')));
      /** retry: 0 comes from the global options, the file lands in the global logDir */
      assert.deepStrictEqual(results, []);
      assert.strictEqual(errors.length, 2);
      assert.ok(fs.readdirSync(_dir).some(f => f.includes('global-opts-errors')));
    });

    it('should let the options of the call win over the global ones', async () => {
      let _executions = 0;
      setDefaultOptions({ retry: 5, logEnabled: false, logQueueStatus: false, logDir: tmpLogDir() });
      await execQueueP('call-wins', ['a'], (el, next) => { _executions += 1; return next(new Error('boom')); }, { retry: 0 });
      /** One round only: the retry of the call replaces the global one */
      assert.strictEqual(_executions, 1);
    });

    it('should replace the previous global options', async () => {
      setDefaultOptions({ retry: 5, logEnabled: false, logQueueStatus: false });
      setDefaultOptions({ logEnabled: false, logQueueStatus: false, logDir: tmpLogDir() });
      let _executions = 0;
      await execQueueP('replaced', ['a'], (el, next) => { _executions += 1; return next(new Error('boom')); });
      /** The retry of the first call is gone, back to the default of 1 retry */
      assert.strictEqual(_executions, 2);
    });

    it('should reset the global options when called without argument', async () => {
      setDefaultOptions({ concurrency: 4, logEnabled: false, logQueueStatus: false });
      setDefaultOptions();
      await execQueueP('reset-opts', [1, 2], (el, next) => next(null, el), silentOptions());
      /** logEnabled is back to true: the START line is logged again */
      assert.ok(_capturedLogs.length === 0);
      await execQueueP('reset-opts-2', [1], (el, next) => next(null, el), { logQueueStatus: false });
      assert.ok(_capturedLogs.some(l => l.includes('[reset-opts-2] START - 1 total elements - 1 queue(s)')));
    });

    it('should copy the given object instead of keeping a reference', async () => {
      const _options = { retry: 0, logEnabled: false, logQueueStatus: false, logDir: tmpLogDir() };
      setDefaultOptions(_options);
      _options.retry = 5;
      let _executions = 0;
      await execQueueP('copied-opts', ['a'], (el, next) => { _executions += 1; return next(new Error('boom')); });
      /** The mutation after the call has no effect */
      assert.strictEqual(_executions, 1);
    });

    it('should ignore the internal keys of the global options', async () => {
      setDefaultOptions({ retry: 0, logEnabled: false, logQueueStatus: false, logDir: tmpLogDir(), _cqueue: true, try: 3, results: ['injected'] });
      const { results } = await execQueue('internal-keys', [1], (el, next) => next(null, el));
      /** `_cqueue`, `try` and `results` are internal: never taken from the global options */
      assert.deepStrictEqual(results, [1]);
    });
  });

  describe('security', () => {
    it('should not let the queue name escape the log directory', async () => {
      const _root = tmpLogDir();
      const _dir = path.join(_root, 'app', 'logs');
      /** A queue name coming from untrusted data must not become a path */
      await execQueueP('../../../pwned', ['x'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir }));
      const _written = fs.readdirSync(_dir);
      assert.strictEqual(_written.length, 1);
      assert.ok(!_written[0].includes('/') && !_written[0].includes('\\'), _written[0]);
      /** Nothing written next to, or above, the log directory */
      assert.deepStrictEqual(fs.readdirSync(path.join(_root, 'app')), ['logs']);
      assert.deepStrictEqual(fs.readdirSync(_root), ['app']);
    });

    it('should neutralize path separators and null bytes in the queue name', async () => {
      const _dir = tmpLogDir();
      await execQueueP('a/b\\c\u0000d', ['x'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir }));
      const _written = fs.readdirSync(_dir);
      assert.strictEqual(_written.length, 1);
      assert.ok(/^.{19}-a-b-c-d-errors\.json$/.test(_written[0]), _written[0]);
    });

    it('should not follow a symlink planted in the log directory', async () => {
      const _dir = tmpLogDir();
      const _victim = path.join(_dir, '..', 'victim.conf');
      fs.writeFileSync(_victim, 'IMPORTANT');
      /** The file name is the timestamp in seconds plus the queue name: a local attacker can
       * predict it and pre-create a symlink to any file the process can write */
      for (let i = 0; i < 3; i++) {
        const _stamp = new Date(Date.now() + i * 1000).toISOString().replace(/:/g, '-').slice(0, 19);
        try {
          fs.symlinkSync(_victim, path.join(_dir, `${_stamp}-victim-queue-errors.json`));
        } catch (err) { /* the name of a previous second already exists */ }
      }
      const { errors } = await execQueueP('victim queue', ['x'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir }));
      assert.strictEqual(fs.readFileSync(_victim, 'utf8'), 'IMPORTANT');
      assert.strictEqual(errors.length, 1);
      /** The planted name is refused, the file is written under a unique name next to it */
      const _written = fs.readdirSync(_dir).filter(f => !fs.lstatSync(path.join(_dir, f)).isSymbolicLink());
      assert.strictEqual(_written.length, 1);
      assert.match(_written[0], new RegExp(`^.{19}-victim-queue-errors-${process.pid}-\\d+\\.json$`));
      assert.strictEqual(JSON.parse(fs.readFileSync(path.join(_dir, _written[0]), 'utf8')).length, 1);
    });

    it('should refuse the write when the unique name is planted as well', async () => {
      const _dir = tmpLogDir();
      const _victim = path.join(_dir, '..', 'victim2.conf');
      fs.writeFileSync(_victim, 'IMPORTANT');
      /** A fresh module: its first collision uses the suffix `-<pid>-1` */
      delete require.cache[require.resolve('../cqueue.js')];
      const _fresh = require('../cqueue.js');
      delete require.cache[require.resolve('../cqueue.js')];
      const _entries = [];
      _fresh.setLogFunction((msg, level, fields) => _entries.push({ msg, level, fields }));
      for (let i = 0; i < 3; i++) {
        const _stamp = new Date(Date.now() + i * 1000).toISOString().replace(/:/g, '-').slice(0, 19);
        for (const _name of [`${_stamp}-victim2-errors.json`, `${_stamp}-victim2-errors-${process.pid}-1.json`]) {
          try {
            fs.symlinkSync(_victim, path.join(_dir, _name));
          } catch (err) { /* already planted */ }
        }
      }
      const { errors } = await _fresh.execQueue('victim2', ['x'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir }));
      assert.strictEqual(fs.readFileSync(_victim, 'utf8'), 'IMPORTANT');
      assert.strictEqual(errors.length, 1);
      const _error = _entries.find(e => e.fields.event === 'log_file_error');
      assert.strictEqual(_error.level, 'error');
      assert.match(_error.fields.error, /EEXIST/);
      /** Nothing announced as created, and the END line does not point to a missing file */
      assert.strictEqual(_entries.filter(e => e.fields.event === 'log_file').length, 0);
      assert.strictEqual(_entries.find(e => e.fields.event === 'queue_failed').msg, '[victim2] END - Stop retrying');
    });

    it('should serialize a throwing entry as null instead of crashing', async () => {
      const _dir = tmpLogDir();
      const _circular = { name: 'evil' };
      _circular.self = _circular;
      /** Past the first chunk: the throw used to happen in a setImmediate, uncatchable by the caller */
      const _logs = Array.from({ length: 600 }, (_, i) => (i === 550 ? _circular : { i: i }));
      _logs[551] = 10n;
      await execQueueP('throwing-entry', [1], (el, next) => next(null, el, { logs: _logs }), silentOptions({ logDir: _dir }));
      const _file = fs.readdirSync(_dir).find(f => f.includes('throwing-entry-logs'));
      const _content = JSON.parse(fs.readFileSync(path.join(_dir, _file), 'utf8'));
      assert.strictEqual(_content.length, 600);
      assert.strictEqual(_content[550], null);
      assert.strictEqual(_content[551], null);
      assert.deepStrictEqual(_content[549], { i: 549 });
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
      await assert.rejects(withInternalError(() => execQueueP('level-crash', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0 }))));
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

  describe('structured log fields', () => {
    it('should pass queue_start and queue_end fields with the round duration', async () => {
      await execQueueP('fields-run', [1, 2, 3, 4], (el, next) => setTimeout(() => next(null, el), 5), { concurrency: 2, delay: 0, retry: 3, logQueueStatus: false });
      const [_start] = entriesOf('queue_start');
      assert.strictEqual(_start.level, 'info');
      assert.ok(_start.msg.startsWith('[fields-run] START - 4 total elements'));
      assert.deepStrictEqual(_start.fields, { event: 'queue_start', queue: 'fields-run', total: 4, concurrency: 2, delay: 0, retry: 3, try: 0 });
      const [_end] = entriesOf('queue_end');
      assert.strictEqual(_end.level, 'info');
      assert.ok(_end.msg.startsWith('[fields-run] END - Duration:'), _end.msg);
      const { duration_ms, avg_ms, ..._rest } = _end.fields;
      assert.deepStrictEqual(_rest, { event: 'queue_end', queue: 'fields-run', total: 4, results: 4, errors: 0, try: 0 });
      /** 2 elements of ~5ms per queue: the wall clock of the round, not a sum of the queues */
      assert.ok(Number.isInteger(duration_ms) && duration_ms >= 8 && duration_ms < 1000, `duration_ms ${duration_ms}`);
      assert.ok(Number.isInteger(avg_ms) && avg_ms >= 4 && avg_ms <= duration_ms, `avg_ms ${avg_ms}`);
    });

    it('should pass queue_errors, queue_retry and queue_failed fields', async () => {
      const _dir = tmpLogDir();
      await execQueueP('fields-fail', ['ok', 'bad1', 'bad2'], (el, next) => next(el === 'ok' ? null : new Error('boom'), el), silentOptions({ retry: 1, logDir: _dir }));
      const _errors = entriesOf('queue_errors');
      assert.strictEqual(_errors.length, 2);
      assert.deepStrictEqual(_errors.map(e => e.level), ['error', 'error']);
      assert.deepStrictEqual(_errors[0].fields, { event: 'queue_errors', queue: 'fields-fail', errors: 2, try: 0, summary: '"Error: boom" x2' });
      assert.strictEqual(_errors[1].fields.try, 1);
      const _retry = entriesOf('queue_retry');
      assert.strictEqual(_retry.length, 1);
      assert.strictEqual(_retry[0].level, 'warn');
      /** `try` is the number of the round about to start */
      assert.deepStrictEqual(_retry[0].fields, { event: 'queue_retry', queue: 'fields-fail', try: 1, elements: 2, delay_ms: 0 });
      const [_failed] = entriesOf('queue_failed');
      assert.strictEqual(_failed.level, 'error');
      assert.strictEqual(_failed.msg, '[fields-fail] END - Stop retrying, check the error file!');
      const { duration_ms, ..._rest } = _failed.fields;
      assert.deepStrictEqual(_rest, { event: 'queue_failed', queue: 'fields-fail', errors: 2, try: 1, total: 2 });
      assert.ok(Number.isInteger(duration_ms) && duration_ms >= 0);
      /** No success line for a failed queue */
      assert.strictEqual(entriesOf('queue_end').length, 0);
    });

    it('should count the results of every round in the queue_end of a recovered queue', async () => {
      let _calls = 0;
      await execQueueP('fields-recover', ['a', 'b', 'c'], (el, next) => (el === 'b' && ++_calls === 1 ? next(new Error('once')) : next(null, el)), { logQueueStatus: false, errorFile: false });
      const _end = entriesOf('queue_end');
      assert.strictEqual(_end.length, 1);
      /** The retry round executed one element, the caller receives the 3 results */
      assert.strictEqual(_end[0].fields.try, 1);
      assert.strictEqual(_end[0].fields.total, 1);
      assert.strictEqual(_end[0].fields.results, 3);
    });

    it('should pass log_file fields with the level of the file', async () => {
      const _dir = tmpLogDir();
      await execQueueP('fields-file', ['a'], (el, next) => next(new Error('boom'), el, { logs: ['hello'] }), silentOptions({ retry: 0, logDir: _dir }));
      const _files = entriesOf('log_file');
      assert.deepStrictEqual(_files.map(e => [e.fields.label, e.level]), [['logs', 'info'], ['errors', 'error']]);
      for (const _entry of _files) {
        assert.deepStrictEqual(Object.keys(_entry.fields), ['event', 'queue', 'label', 'path']);
        assert.strictEqual(_entry.fields.queue, 'fields-file');
        assert.strictEqual(path.dirname(_entry.fields.path), _dir);
        assert.ok(fs.existsSync(_entry.fields.path));
      }
    });

    it('should pass log_file_error fields with the message of the failure', async (t) => {
      if (process.getuid && process.getuid() === 0) {
        return t.skip('root bypasses the folder permissions');
      }
      const _dir = tmpLogDir();
      fs.chmodSync(_dir, 0o500);
      try {
        await execQueueP('fields-nowrite', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir }));
      } finally {
        fs.chmodSync(_dir, 0o700);
      }
      const [_error] = entriesOf('log_file_error');
      assert.strictEqual(_error.level, 'error');
      assert.strictEqual(_error.fields.queue, 'fields-nowrite');
      assert.strictEqual(_error.fields.label, 'errors');
      assert.strictEqual(path.dirname(_error.fields.path), _dir);
      assert.match(_error.fields.error, /EACCES/);
    });

    it('should pass log_cleanup_error fields at the warn level', async () => {
      const _dir = tmpLogDir();
      const _stamp = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().replace(/:/g, '-').slice(0, 19);
      fs.mkdirSync(path.join(_dir, `${_stamp}-fields-cleanup-errors.json`));
      await execQueueP('fields-cleanup', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir, logRetentionDays: 7 }));
      const [_warn] = entriesOf('log_cleanup_error');
      assert.strictEqual(_warn.level, 'warn');
      assert.strictEqual(_warn.fields.queue, 'fields-cleanup');
      assert.strictEqual(_warn.fields.path, path.join(_dir, `${_stamp}-fields-cleanup-errors.json`));
      assert.strictEqual(typeof _warn.fields.error, 'string');
    });

    it('should pass queue_crash fields on an internal error', async () => {
      await assert.rejects(withInternalError(() => execQueueP(123, ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0 }))));
      const [_crash] = entriesOf('queue_crash');
      assert.strictEqual(_crash.level, 'error');
      /** The queue name is always passed as a string */
      assert.strictEqual(_crash.fields.queue, '123');
      assert.strictEqual(_crash.fields.try, 0);
      assert.strictEqual(_crash.fields.error, 'TypeError: internal failure');
    });

    it('should pass aggregated queue_progress fields without a TTY', async () => {
      await execQueueP('fields-progress', [1, 2, 3, 4, 5], (el, next) => next(null, el), { concurrency: 2, logEnabled: false, logQueueStatus: true });
      const _progress = entriesOf('queue_progress');
      assert.ok(_progress.length >= 2, `got ${_progress.length} progress entries`);
      /** The message is the unchanged multi-line status block */
      assert.ok(_progress.at(-1).msg.startsWith('[0] 100% - 3/3'));
      assert.deepStrictEqual(_progress.at(-1).fields, { event: 'queue_progress', queue: 'fields-progress', try: 0, queues: 2, done: 5, total: 5, percent: 100, eta_ms: 0, errors: 0 });
      assert.ok(_progress[0].fields.done < 5);
    });

    it('should always pass a flat object of primitives with event and queue', async () => {
      const _dir = tmpLogDir();
      await execQueueP('fields-flat', ['ok', 'bad'], (el, next) => next(el === 'bad' ? new Error('boom') : null, { nested: el }, { logs: [{ el }] }),
        { logQueueStatus: true, retry: 1, logDir: _dir, logRetentionDays: 7 });
      assert.ok(_capturedEntries.length >= 8, `got ${_capturedEntries.length} entries`);
      for (const _entry of _capturedEntries) {
        assert.ok(_entry.fields && Object.getPrototypeOf(_entry.fields) === Object.prototype, _entry.msg);
        assert.strictEqual(typeof _entry.fields.event, 'string', _entry.msg);
        assert.strictEqual(_entry.fields.queue, 'fields-flat', _entry.msg);
        for (const [_key, _value] of Object.entries(_entry.fields)) {
          assert.ok(['string', 'number', 'boolean'].includes(typeof _value), `${_entry.fields.event}.${_key} is ${typeof _value}`);
        }
      }
    });

    it('should keep a 2 arguments logger and the default console output working', async () => {
      delete require.cache[require.resolve('../cqueue.js')];
      const _fresh = require('../cqueue.js');
      delete require.cache[require.resolve('../cqueue.js')];
      const _legacy = [];
      _fresh.setLogFunction(function (msg, level) { _legacy.push(`${level}|${msg}`); });
      await _fresh.execQueue('legacy', ['bad'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: tmpLogDir() }));
      assert.ok(_legacy.includes('error|[legacy] 1 errors: "Error: boom" x1'), _legacy.join('\n'));
      assert.ok(_legacy.includes('error|[legacy] END - Stop retrying, check the error file!'), _legacy.join('\n'));

      /** The default output receives the message only: the fields are never printed as an object */
      delete require.cache[require.resolve('../cqueue.js')];
      const _default = require('../cqueue.js');
      delete require.cache[require.resolve('../cqueue.js')];
      const _calls = [];
      const _consoleLog = console.log;
      console.log = (...args) => _calls.push(args);
      try {
        await _default.execQueue('default-fields', [1], (el, next) => next(null, el), { logQueueStatus: false });
      } finally {
        console.log = _consoleLog;
      }
      assert.ok(_calls.length >= 2);
      assert.ok(_calls.every(args => args.length === 1 && typeof args[0] === 'string'), JSON.stringify(_calls));
    });

    it('should complete the queue when the host logger throws', async () => {
      delete require.cache[require.resolve('../cqueue.js')];
      const _fresh = require('../cqueue.js');
      delete require.cache[require.resolve('../cqueue.js')];
      _fresh.setLogFunction(() => { throw new Error('logger down'); });
      const { results, errors } = await _fresh.execQueue('throwing-logger', ['ok', 'bad'], (el, next) => (el === 'bad' ? next(new Error('boom')) : next(null, el)),
        { logQueueStatus: true, retry: 1, logDir: tmpLogDir() });
      assert.deepStrictEqual(results, ['ok']);
      assert.strictEqual(errors.length, 1);
    });
  });

  describe('errorFile option', () => {
    it('should never write the errors file when false, the summary is still logged', async () => {
      const _dir = tmpLogDir();
      const { errors } = await execQueueP('no-error-file', ['bad'], (el, next) => next(new Error('boom')), silentOptions({ retry: 1, logDir: _dir, errorFile: false }));
      assert.strictEqual(errors.length, 1);
      assert.deepStrictEqual(fs.readdirSync(_dir), []);
      assert.strictEqual(entriesOf('log_file').length, 0);
      assert.strictEqual(entriesOf('queue_errors').length, 2);
      assert.strictEqual(entriesOf('queue_errors')[1].fields.summary, '"Error: boom" x1');
      /** No file to check: the end line does not point to one */
      assert.strictEqual(entriesOf('queue_failed')[0].msg, '[no-error-file] END - Stop retrying');
    });

    it('should still write the logs file when errorFile is false', async () => {
      const _dir = tmpLogDir();
      await execQueueP('logs-only', ['bad'], (el, next) => next(new Error('boom'), null, { logs: ['kept'] }), silentOptions({ retry: 0, logDir: _dir, errorFile: false }));
      const _files = fs.readdirSync(_dir);
      assert.strictEqual(_files.length, 1);
      assert.ok(_files[0].endsWith('-logs-only-logs.json'), _files[0]);
    });

    it('should write only the errors file of the last round with \'final\'', async () => {
      const _dir = tmpLogDir();
      await execQueueP('final-file', ['bad1', 'bad2'], (el, next) => next(new Error('boom')), silentOptions({ retry: 2, logDir: _dir, errorFile: 'final' }));
      const _files = fs.readdirSync(_dir);
      assert.strictEqual(_files.length, 1);
      assert.ok(_files[0].endsWith('-final-file-errors-try2.json'), _files[0]);
      assert.strictEqual(JSON.parse(fs.readFileSync(path.join(_dir, _files[0]), 'utf8')).length, 2);
      assert.strictEqual(entriesOf('queue_failed')[0].msg, '[final-file] END - Stop retrying, check the error file!');
    });

    it('should write no errors file with \'final\' when the retry recovers', async () => {
      const _dir = tmpLogDir();
      let _calls = 0;
      const { errors } = await execQueueP('final-recover', ['a'], (el, next) => next(++_calls === 1 ? new Error('once') : null, el), silentOptions({ retry: 1, logDir: _dir, errorFile: 'final' }));
      assert.deepStrictEqual(errors, []);
      assert.deepStrictEqual(fs.readdirSync(_dir), []);
    });

    it('should write the errors file of the only round with \'final\' and no retry', async () => {
      const _dir = tmpLogDir();
      await execQueueP('final-noretry', ['bad'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir, errorFile: 'final' }));
      assert.strictEqual(fs.readdirSync(_dir).length, 1);
    });

    it('should read errorFile from setDefaultOptions', async () => {
      const _dir = tmpLogDir();
      setDefaultOptions({ errorFile: false, logDir: _dir });
      await execQueueP('default-no-file', ['bad'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0 }));
      assert.deepStrictEqual(fs.readdirSync(_dir), []);
      /** The option of the call wins */
      await execQueueP('call-file', ['bad'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, errorFile: true }));
      assert.strictEqual(fs.readdirSync(_dir).length, 1);
    });
  });

  describe('retryDelay option', () => {
    it('should wait an exponential delay before each retry round', async () => {
      const _rounds = [];
      const _start = Date.now();
      await execQueueP('retry-delay', ['bad'], (el, next) => { _rounds.push(Date.now() - _start); next(new Error('boom')); },
        silentOptions({ retry: 2, retryDelay: 30, errorFile: false }));
      assert.deepStrictEqual(entriesOf('queue_retry').map(e => e.fields.delay_ms), [30, 60]);
      assert.strictEqual(_rounds.length, 3);
      /** Timers can fire a little early on some platforms: 1ms of tolerance */
      assert.ok(_rounds[1] - _rounds[0] >= 29, `first retry after ${_rounds[1] - _rounds[0]}ms`);
      assert.ok(_rounds[2] - _rounds[1] >= 59, `second retry after ${_rounds[2] - _rounds[1]}ms`);
    });

    it('should not wait without retryDelay, nor with an invalid value', async () => {
      for (const _retryDelay of [undefined, 0, -100, 'abc', NaN, null]) {
        _capturedEntries = [];
        await execQueueP('retry-nodelay', ['bad'], (el, next) => next(new Error('boom')), silentOptions({ retry: 1, retryDelay: _retryDelay, errorFile: false }));
        assert.strictEqual(entriesOf('queue_retry')[0].fields.delay_ms, 0, String(_retryDelay));
      }
    });

    it('should cap the delay to 60 seconds', async () => {
      /** Long timers are shortened to 0ms while recording the requested duration */
      const _requested = [];
      const _setTimeout = global.setTimeout;
      global.setTimeout = (fn, ms, ...args) => {
        if (ms >= 1000) {
          _requested.push(ms);
          ms = 0;
        }
        return _setTimeout(fn, ms, ...args);
      };
      try {
        await execQueueP('retry-cap', ['bad'], (el, next) => next(new Error('boom')), silentOptions({ retry: 3, retryDelay: 25000, errorFile: false }));
        await execQueueP('retry-infinite', ['bad'], (el, next) => next(new Error('boom')), silentOptions({ retry: 1, retryDelay: Infinity, errorFile: false }));
      } finally {
        global.setTimeout = _setTimeout;
      }
      assert.deepStrictEqual(_requested, [25000, 50000, 60000, 60000]);
      assert.deepStrictEqual(entriesOf('queue_retry').map(e => e.fields.delay_ms), [25000, 50000, 60000, 60000]);
    });

    it('should read retryDelay from setDefaultOptions', async () => {
      setDefaultOptions({ retryDelay: 15 });
      await execQueueP('default-retry-delay', ['bad'], (el, next) => next(new Error('boom')), silentOptions({ retry: 1, errorFile: false }));
      assert.strictEqual(entriesOf('queue_retry')[0].fields.delay_ms, 15);
    });
  });

  describe('log injection', () => {
    it('should keep the error summary on a single line and quote each message as JSON', async () => {
      await execQueueP('inject', ['a'], (el, next) => next(new Error('bad\r\nERROR [inject] forged "line" \x85\\')), silentOptions({ retry: 0, errorFile: false }));
      const [_errors] = entriesOf('queue_errors');
      assert.strictEqual(_errors.msg, '[inject] 1 errors: "Error: bad  ERROR [inject] forged \\"line\\"  \\\\" x1');
      assert.strictEqual(_errors.fields.summary, '"Error: bad  ERROR [inject] forged \\"line\\"  \\\\" x1');
    });

    it('should neutralize the control characters of the queue name in messages and fields', async () => {
      await execQueueP('evil\nWARN [x] forged\x1b[2J', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 1, errorFile: false }));
      assert.ok(_capturedEntries.length >= 4);
      for (const _entry of _capturedEntries) {
        assert.ok(!/[\x00-\x1f]/.test(_entry.msg), JSON.stringify(_entry.msg));
        assert.strictEqual(_entry.fields.queue, 'evil WARN [x] forged [2J');
      }
    });

    it('should keep the crash message and its error field on a single line', async () => {
      const _err = await withInternalError(() => execQueueP('crash-inject', ['a'], (el, next) => next(null, el), silentOptions()), 'bad\nERROR forged').catch(e => e);
      assert.strictEqual(_err, '[crash-inject] Error: Promise All Catched: TypeError: bad ERROR forged');
      const [_crash] = entriesOf('queue_crash');
      assert.strictEqual(_crash.msg, _err);
      assert.strictEqual(_crash.fields.error, 'TypeError: bad ERROR forged');
    });

    it('should keep the full error message in the error file', async () => {
      const _dir = tmpLogDir();
      await execQueueP('inject-file', ['a'], (el, next) => next(new Error('multi\nline')), silentOptions({ retry: 0, logDir: _dir }));
      const _content = JSON.parse(fs.readFileSync(path.join(_dir, fs.readdirSync(_dir)[0]), 'utf8'));
      assert.deepStrictEqual(_content, [{ element: 'a', message: 'Error: multi\nline' }]);
    });
  });

  describe('callback that throws', () => {
    /** The throw is not caught: it rejects the promise returned by execQueue, exactly like a throw
     * in the callback of a successful run, and the callback is never called a second time */
    for (const [_title, _worker, _options] of [
      ['remaining errors', (el, next) => next(new Error('boom')), { retry: 0 }],
      ['remaining errors after a retry round', (el, next) => next(new Error('boom')), { retry: 1 }],
      ['success', (el, next) => next(null, el), { retry: 0 }]
    ]) {
      it(`should call the callback once and propagate its throw (${_title})`, async () => {
        let _calls = 0;
        const _promise = execQueue('cb-throws', ['a'], _worker, silentOptions(Object.assign({ errorFile: false }, _options)), () => {
          _calls += 1;
          throw new Error('user bug');
        });
        await assert.rejects(_promise, /user bug/);
        assert.strictEqual(_calls, 1);
        assert.strictEqual(entriesOf('queue_crash').length, 0);
      });
    }
  });

  describe('file name collisions', () => {
    it('should keep both files of two runs of the same queue in the same second', async () => {
      const _dir = tmpLogDir();
      const _fail = (el, next) => next(new Error('boom'));
      await Promise.all([
        execQueueP('twin', ['a'], _fail, silentOptions({ retry: 0, logDir: _dir })),
        execQueueP('twin', ['b'], _fail, silentOptions({ retry: 0, logDir: _dir }))
      ]);
      const _files = fs.readdirSync(_dir);
      assert.strictEqual(_files.length, 2, _files.join(','));
      const _elements = _files.map(f => JSON.parse(fs.readFileSync(path.join(_dir, f), 'utf8'))[0].element).sort();
      assert.deepStrictEqual(_elements, ['a', 'b']);
      assert.strictEqual(entriesOf('log_file_error').length, 0);
    });

    it('should write under a unique name when the name is already taken', async () => {
      const _dir = tmpLogDir();
      for (let i = 0; i < 3; i++) {
        const _stamp = new Date(Date.now() + i * 1000).toISOString().replace(/:/g, '-').slice(0, 19);
        fs.writeFileSync(path.join(_dir, `${_stamp}-taken-errors.json`), 'PREVIOUS');
      }
      await execQueueP('taken', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir }));
      const [_file] = entriesOf('log_file');
      assert.match(path.basename(_file.fields.path), new RegExp(`^.{19}-taken-errors-${process.pid}-\\d+\\.json$`));
      assert.strictEqual(JSON.parse(fs.readFileSync(_file.fields.path, 'utf8')).length, 1);
      /** The existing files are untouched */
      assert.strictEqual(fs.readdirSync(_dir).filter(f => fs.readFileSync(path.join(_dir, f), 'utf8') === 'PREVIOUS').length, 3);
    });

    it('should apply the retention to the files with a unique suffix', async () => {
      const _dir = tmpLogDir();
      const _old = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().replace(/:/g, '-').slice(0, 19);
      const _expired = [`${_old}-ret-errors-123-4.json`, `${_old}-ret-errors-try1-123-5.json`];
      const _kept = [`${_old}-ret-errors-x.json`, `${_old}-other-ret-errors-123-4.json`];
      for (const _name of [..._expired, ..._kept]) {
        fs.writeFileSync(path.join(_dir, _name), '[]');
      }
      await execQueueP('ret', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir, logRetentionDays: 7 }));
      const _files = fs.readdirSync(_dir);
      for (const _name of _expired) {
        assert.ok(!_files.includes(_name), _name);
      }
      for (const _name of _kept) {
        assert.ok(_files.includes(_name), _name);
      }
    });
  });

  describe('file write failures', () => {
    it('should stop serializing and not announce the file when the stream fails', async () => {
      const _dir = tmpLogDir();
      const _createWriteStream = fs.createWriteStream;
      /** Every write fails, like a full disk */
      fs.createWriteStream = (...args) => {
        const _stream = _createWriteStream(...args);
        _stream._write = (chunk, encoding, cb) => cb(new Error('ENOSPC: no space left on device'));
        return _stream;
      };
      let _serialized = 0;
      const _logs = Array.from({ length: 5000 }, () => ({ toJSON () { _serialized += 1; return 1; } }));
      try {
        await execQueueP('disk-full', ['a'], (el, next) => next(new Error('boom'), null, { logs: _logs }), silentOptions({ retry: 0, logDir: _dir }));
      } finally {
        fs.createWriteStream = _createWriteStream;
      }
      /** The first chunk only, not the 10 chunks of the content */
      assert.ok(_serialized < _logs.length, `${_serialized} entries serialized`);
      assert.deepStrictEqual(entriesOf('log_file_error').map(e => e.fields.label), ['logs', 'errors']);
      assert.strictEqual(entriesOf('log_file').length, 0);
      assert.strictEqual(entriesOf('queue_failed')[0].msg, '[disk-full] END - Stop retrying');
    });
  });

  describe('queue name', () => {
    it('should accept a number as queue name', async () => {
      const _dir = tmpLogDir();
      const { errors } = await execQueueP(42, ['a'], (el, next) => next(new Error('boom'), null, { logs: ['x'] }), silentOptions({ retry: 0, logDir: _dir, logRetentionDays: 1 }));
      assert.strictEqual(errors.length, 1);
      assert.deepStrictEqual(fs.readdirSync(_dir).map(f => f.slice(20)).sort(), ['42-errors.json', '42-logs.json']);
      assert.strictEqual(entriesOf('queue_errors')[0].msg, '[42] 1 errors: "Error: boom" x1');
      assert.strictEqual(entriesOf('queue_crash').length, 0);
    });
  });

  describe('async logger', () => {
    it('should not leave an unhandled rejection when the logger returns a rejected promise', async () => {
      delete require.cache[require.resolve('../cqueue.js')];
      const _fresh = require('../cqueue.js');
      delete require.cache[require.resolve('../cqueue.js')];
      const _unhandled = [];
      const _onUnhandled = (reason) => _unhandled.push(reason);
      process.on('unhandledRejection', _onUnhandled);
      try {
        _fresh.setLogFunction(async () => { throw new Error('async logger down'); });
        const { errors } = await _fresh.execQueue('async-logger', ['bad'], (el, next) => next(new Error('boom')), { logQueueStatus: true, retry: 1, logDir: tmpLogDir() });
        assert.strictEqual(errors.length, 1);
        /** unhandledRejection is emitted after the microtask queue drains */
        await new Promise(resolve => setTimeout(resolve, 20));
      } finally {
        process.removeListener('unhandledRejection', _onUnhandled);
      }
      assert.deepStrictEqual(_unhandled, []);
    });
  });

  describe('hardening', () => {
    /** Run execQueue and watch for an uncaught exception or an unhandled rejection meanwhile */
    async function withoutStrayErrors (fn) {
      const _stray = [];
      const _onStray = (err) => _stray.push(err);
      process.on('unhandledRejection', _onStray);
      process.on('uncaughtException', _onStray);
      try {
        const _res = await fn();
        await new Promise(resolve => setTimeout(resolve, 20));
        assert.deepStrictEqual(_stray, []);
        return _res;
      } finally {
        process.removeListener('unhandledRejection', _onStray);
        process.removeListener('uncaughtException', _onStray);
      }
    }

    it('should record an error that cannot be converted to a string, from a sync or an async worker', async () => {
      const _throwingToString = { toString () { throw new Error('no string'); } };
      for (const _async of [false, true]) {
        for (const _err of [Object.create(null), _throwingToString]) {
          const { results, errors } = await withoutStrayErrors(() => execQueueP('bad-error', ['a', 'b'], (el, next) => {
            const _done = () => (el === 'a' ? next(_err) : next(null, el));
            return _async ? setImmediate(_done) : _done();
          }, silentOptions({ retry: 0, errorFile: false })));
          assert.deepStrictEqual(results, ['b']);
          assert.deepStrictEqual(errors, [{ element: 'a', message: 'Unknown error' }]);
        }
      }
    });

    it('should finish the element when the actions getters throw', async () => {
      const _actions = { get logs () { throw new Error('getter'); } };
      const { results, errors } = await withoutStrayErrors(() => execQueueP('bad-actions', [1, 2], (el, next) => next(el === 1 ? new Error('boom') : null, el, _actions), silentOptions({ retry: 0, errorFile: false })));
      assert.deepStrictEqual(results, [1, 2]);
      /** The message is read before the throwing getter: it is kept */
      assert.deepStrictEqual(errors, [{ element: 1, message: 'Error: boom' }]);
    });

    it('should fall back to the default retry for a non-numeric or infinite value', async () => {
      for (const _retry of [NaN, 'abc', {}, Infinity, -1]) {
        let _calls = 0;
        const { errors } = await execQueueP('bad-retry', ['x'], (el, next) => { _calls += 1; return next(new Error('boom')); }, silentOptions({ retry: _retry, errorFile: false }));
        assert.strictEqual(errors.length, 1);
        /** Default retry: 1, so two executions */
        assert.strictEqual(_calls, 2, String(_retry));
      }
    });

    it('should yield to the event loop between retry rounds of a synchronous failing worker', async () => {
      let _ticked = false;
      let _tickedDuringRetries = false;
      setImmediate(() => { _ticked = true; });
      await execQueueP('retry-yield', ['x'], (el, next) => {
        _tickedDuringRetries = _tickedDuringRetries || _ticked;
        return next(new Error('boom'));
      }, silentOptions({ retry: 3, errorFile: false }));
      assert.strictEqual(_tickedDuringRetries, true);
    });

    it('should fall back to the defaults for an invalid delay, retryDelay or logRetentionDays', async () => {
      const _dir = tmpLogDir();
      fs.writeFileSync(path.join(_dir, '2000-01-01T00-00-00-bad-numbers-errors.json'), '[]');
      const { results, errors } = await execQueueP('bad-numbers', [1, 2, 'x'], (el, next) => (el === 'x' ? next(new Error('boom')) : next(null, el)),
        silentOptions({ retry: 1, delay: 'abc', retryDelay: {}, logRetentionDays: NaN, logDir: _dir, logEnabled: true }));
      assert.deepStrictEqual(results, [1, 2]);
      assert.strictEqual(errors.length, 1);
      assert.strictEqual(entriesOf('queue_start')[0].fields.delay, 0);
      assert.strictEqual(entriesOf('queue_retry')[0].fields.delay_ms, 0);
      /** logRetentionDays falls back to 0: every file is kept */
      assert.ok(fs.existsSync(path.join(_dir, '2000-01-01T00-00-00-bad-numbers-errors.json')));
    });

    it('should process every element with an invalid concurrency', async () => {
      for (const _concurrency of [NaN, 'abc', {}, Infinity, -3]) {
        const { results } = await withoutStrayErrors(() => execQueueP('bad-concurrency', [1, 2, 3], (el, next) => next(null, el), silentOptions({ concurrency: _concurrency })));
        assert.deepStrictEqual(results, [1, 2, 3], String(_concurrency));
      }
    });

    it('should never start more queues than elements', async () => {
      const { results } = await execQueueP('huge-concurrency', [1, 2, 3], (el, next) => next(null, el), { logEnabled: false, logQueueStatus: true, concurrency: 20000 });
      assert.deepStrictEqual(results, [1, 2, 3]);
      assert.strictEqual(entriesOf('queue_progress').at(-1).fields.queues, 3);
    });

    it('should print the status without a TTY every 10% of the round, whatever the number of queues', async () => {
      const _list = Array.from({ length: 5000 }, (_, i) => i);
      const { results } = await execQueueP('many-queues', _list, (el, next) => next(null, el), { logEnabled: false, logQueueStatus: true, concurrency: 5000 });
      assert.strictEqual(results.length, 5000);
      const _progress = entriesOf('queue_progress');
      /** One block per 10% step plus the final one, instead of one per queue reaching 100% */
      assert.ok(_progress.length <= 12, `got ${_progress.length} status prints`);
      assert.strictEqual(_progress.at(-1).fields.percent, 100);
    });

    it('should create the files readable by the owner only, and a new log directory private', async () => {
      const _dir = path.join(tmpLogDir(), 'new');
      await execQueueP('private-files', [{ password: 's3cret' }], (el, next) => next(new Error('boom'), null, { logs: ['token'] }), silentOptions({ retry: 0, logDir: _dir }));
      assert.strictEqual(fs.statSync(_dir).mode & 0o777, 0o700);
      const _files = fs.readdirSync(_dir);
      assert.strictEqual(_files.length, 2);
      for (const _file of _files) {
        assert.strictEqual(fs.statSync(path.join(_dir, _file)).mode & 0o777, 0o600, _file);
      }
      /** An existing directory is left as it is */
      const _existing = tmpLogDir();
      fs.chmodSync(_existing, 0o755);
      await execQueueP('existing-dir', ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _existing }));
      assert.strictEqual(fs.statSync(_existing).mode & 0o777, 0o755);
    });

    it('should return invalid arguments through the callback, or reject in promise mode', async () => {
      const _nullName = Object.create(null);
      for (const [_name, _list, _options] of [['q', null, {}], ['q', 42, {}], ['q', [1], { logDir: {} }], [_nullName, [1], {}]]) {
        const _err = await withoutStrayErrors(() => new Promise(resolve => {
          execQueue(_name, _list, (el, next) => next(null, el), silentOptions(_options), (err) => resolve(err));
        }));
        assert.ok(_err instanceof TypeError, String(_err));
        await assert.rejects(execQueue(_name, _list, (el, next) => next(null, el), silentOptions(_options)), TypeError);
      }
    });

    it('should not let the caller forge the internal keys', async () => {
      const { results } = await execQueueP('forged-keys', [1], (el, next) => next(null, el),
        { logQueueStatus: false, _cqueue: true, try: 3, results: ['injected'], concurrency: 'abc' });
      assert.deepStrictEqual(results, [1]);
      assert.strictEqual(entriesOf('queue_start')[0].msg, '[forged-keys] START - 1 total elements - 1 queue(s) - 0ms delay - 0/1 retrie(s)');
    });

    it('should keep the current logger when setLogFunction receives a non-function', async () => {
      delete require.cache[require.resolve('../cqueue.js')];
      const _fresh = require('../cqueue.js');
      delete require.cache[require.resolve('../cqueue.js')];
      const _messages = [];
      _fresh.setLogFunction((msg) => _messages.push(msg));
      _fresh.setLogFunction('not a function');
      await _fresh.execQueue('kept-logger', [1], (el, next) => next(null, el), { logQueueStatus: false });
      assert.ok(_messages.some(m => m.startsWith('[kept-logger] END')), _messages.join('|'));
    });

    it('should replace the bidi controls of the queue name in the logs', async () => {
      await execQueueP('a‎b‏c‪d‮e⁦f⁩g', [1], (el, next) => next(null, el), { logQueueStatus: false });
      assert.strictEqual(entriesOf('queue_start')[0].fields.queue, 'a b c d e f g');
    });

    it('should truncate a long queue name in the file name', async () => {
      const _dir = tmpLogDir();
      const { errors } = await execQueueP('x'.repeat(100000), ['a'], (el, next) => next(new Error('boom')), silentOptions({ retry: 0, logDir: _dir, logRetentionDays: 1 }));
      assert.strictEqual(errors.length, 1);
      assert.strictEqual(entriesOf('log_file_error').length, 0);
      const _files = fs.readdirSync(_dir);
      assert.strictEqual(_files.length, 1);
      assert.match(_files[0], /^.{19}-x{200}-errors\.json$/);
    });
  });
});
