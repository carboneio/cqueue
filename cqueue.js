const fs = require('fs');
const path = require('path');
const NS_PER_SEC = 1e9;
const MS_PER_NS = 1e-6;

/** Consecutive synchronous executions yield back to the event loop every X ms */
const YIELD_INTERVAL_MS = 8;
/** On a TTY, the queue status block is re-painted at most every X ms */
const RENDER_INTERVAL_MS = 200;
/** Without a TTY, the queue status is printed only when a queue progresses by X percent */
const RENDER_PERCENT_STEP = 10;
/** Number of elements serialized per event loop tick when writing a log file */
const LOG_FILE_CHUNK_SIZE = 500;
/** Retention is expressed in days */
const MS_PER_DAY = 24 * 60 * 60 * 1000;
/** Number of distinct error messages detailed in the error summary line */
const ERROR_SUMMARY_MAX_ENTRIES = 3;
/** Each message of the error summary is truncated to X characters to keep the line readable */
const ERROR_SUMMARY_MAX_MESSAGE_LENGTH = 100;
/** Upper bound of the exponential wait before a retry round. Also keeps the value far below the
 * 2^31-1 ms limit of setTimeout, above which Node.js fires the timer immediately */
const RETRY_DELAY_MAX_MS = 60 * 1000;
/** C0 / DEL / C1 control characters, the Unicode line separators and the bidi controls: a CR or LF
 * coming from a queue name or an error message would forge a fake line in a line-based log, and a
 * bidi control would display the line in a different order than the one written */
const CONTROL_CHARS = /[\x00-\x1f\x7f-\x9f\u200e\u200f\u2028-\u202e\u2066-\u2069]/g;
/** The queue name part of a file name is truncated to X characters: a long name would fail with ENAMETOOLONG */
const QUEUE_SLUG_MAX_LENGTH = 200; // 19 (date) + 200 + label and suffix stay under the 255 bytes of a file name, and the files of existing queues keep their name
/** Marks an options object already normalized by execQueue: module-private, a caller cannot forge it */
const NORMALIZED = Symbol('cqueue normalized options');

/** Options applied to every execQueue call, replaceable with setDefaultOptions */
let defaultOptions = {};

/** Status block currently painted on the TTY, erased/repainted around log messages */
let activeStatus = null;

/** Incremented on each same-second file name collision, part of the unique suffix of the retried name */
let logFileCollisions = 0;

/** Single-line string for the logs: control characters are replaced by a space */
function toLogText (value) {
  return String(value).replace(CONTROL_CHARS, ' ');
}

function unpackGenericQueue(index, lists, functionToExecute, timeout, callback) {
  if (!callback) {
    callback = timeout;
    timeout = 10;
  }

  const _queue = lists[index];
  const _queueTime = _queue.time;
  const _listLength = _queue.listLength
  let _lastYield = process.hrtime.bigint();

  function finish () {
    _queue.done = true;
    /** A counter instead of a scan of every queue: O(1) per queue */
    lists.finished += 1;
    /** The very last queue forces a final render so the status always shows the completed state */
    printQueueStatus(lists, lists.finished === lists.length);
    return callback(null, { errors: _queue.errors, results: _queue.results, logs: _queue.logs });
  }

  function resumeAfterYield () {
    _lastYield = process.hrtime.bigint();
    run();
  }

  function scheduleNext () {
    if (timeout > 0) {
      return setTimeout(run, timeout);
    }
    if (Number(process.hrtime.bigint() - _lastYield) * MS_PER_NS >= YIELD_INTERVAL_MS) {
      return setImmediate(resumeAfterYield);
    }
    return run();
  }

  function run () {
    /** Elements completing synchronously are executed in a loop instead of one setImmediate each:
     * the loop yields back to the event loop every YIELD_INTERVAL_MS to never starve timers and I/O */
    while (true) {
      if (lists.stopped === true || _queue.cursor >= _listLength) {
        return finish();
      }
      /** The cursor avoids Array.shift() which is O(n) for each element, O(n²) for the whole chunk */
      const _element = _queue.list[_queue.cursor];
      const _start = process.hrtime.bigint();
      let _handled = false;
      let _syncWindow = true;
      let _completedSync = false;
      const _onItemDone = (err, res, actions) => {
        /** Guard: ignore a second call of the callback for the same element */
        if (_handled === true) {
          return;
        }
        /** Read before the element is marked as handled: an error without toString (Object.create(null)),
         * a throwing toString or a throwing getter must not leave the element, and the queue, unfinished */
        let _message = 'Unknown error';
        let _logs = [];
        let _stop = false;
        try {
          if (err) {
            _message = String(err.toString());
          }
          if (actions?.logs instanceof Array) {
            _logs = actions.logs;
          }
          _stop = actions?.stop === true;
        } catch (e) {
          /** The defaults above are kept */
        }
        _handled = true;
        if (err) {
          _queue.errors.push({
            element: _element,
            message: _message
          });
        }
        /** Keep any provided result, including falsy values such as 0, '' or false */
        if (res !== undefined && res !== null) {
          _queue.results.push(res);
        }
        /** A loop instead of push(...spread): a spread of a huge array can overflow the call stack */
        for (let i = 0; i < _logs.length; i++) {
          _queue.logs.push(_logs[i]);
        }
        if (_stop === true) {
          /** Stop ALL queues: the shared flag is checked by every queue before executing its next element */
          lists.stopped = true;
        } else {
          _queue.cursor += 1;
          lists.processed += 1;
        }
        _queueTime.requestTime = Math.round(Number(process.hrtime.bigint() - _start) * MS_PER_NS);
        _queueTime.passedTime += _queueTime.requestTime;
        _queueTime.averageTime = Math.round(_queueTime.averageTime + ((_queueTime.requestTime - _queueTime.averageTime) / (_queue.cursor === 0 ? 1 : _queue.cursor)));
        _queueTime.leftTime = _queueTime.averageTime * (_listLength - _queue.cursor);
        _queueTime.done = _queue.cursor;
        _queueTime.percentage = Math.round((_queueTime.done) * 100 / _listLength);
        printQueueStatus(lists);
        if (_syncWindow === true) {
          /** The element completed synchronously: tell the loop to continue */
          _completedSync = true;
          return;
        }
        return scheduleNext();
      };
      try {
        functionToExecute(_element, _onItemDone);
      } catch (err) {
        /** A synchronous throw is recorded as an error instead of crashing the process */
        _onItemDone(err);
      }
      _syncWindow = false;
      if (_completedSync === false) {
        /** Asynchronous element: _onItemDone resumes the loop when it completes */
        return;
      }
      if (timeout > 0) {
        return void setTimeout(run, timeout);
      }
      if (Number(process.hrtime.bigint() - _lastYield) * MS_PER_NS >= YIELD_INTERVAL_MS) {
        return void setImmediate(resumeAfterYield);
      }
    }
  }

  run();
}

function unpackGenericQueuePromisify(index, list, functionToExecute, execDelay) {
  return new Promise (function (resolve, reject) {
    unpackGenericQueue(index, list, functionToExecute, execDelay, (err, res) => {
      if (err) {
        return reject(err);
      }
      return resolve(res);
    })
  });
}

function msToTime(ms) {
  /** Thresholds are compared on the raw ms value, so 999ms prints "999 ms" and not "1.0 Sec" */
  if (ms < 1000) return ms + " ms";
  else if (ms < 1000 * 60) return (ms / 1000).toFixed(1) + " Sec";
  else if (ms < 1000 * 60 * 60) return (ms / (1000 * 60)).toFixed(1) + " Min";
  else if (ms < 1000 * 60 * 60 * 24) return (ms / (1000 * 60 * 60)).toFixed(1) + " Hrs";
  else return (ms / (1000 * 60 * 60 * 24)).toFixed(1) + " Days"
}

function getStatusLine (queue) {
  const _time = queue.time;
  let _line = `[${queue.id}] ${_time.percentage}% - ${_time.done}/${queue.listLength} - Passed time: ${msToTime(_time.passedTime)} | Left Time: ${msToTime(_time.leftTime)} | Avg time/exec: ${msToTime(_time.averageTime)}`;
  if (queue.done === true) {
    _line += ' ✅ Done';
  }
  if (queue.errors.length > 0) {
    _line += ` 🚩 ${queue.errors.length} errors`;
  }
  return _line;
}

function paintStatusTTY (lists, state) {
  const _columns = process.stdout.columns || 120;
  /** The whole block is a single write: cursor hidden during the repaint, restored at the end */
  let _out = '\x1b[?25l';
  if (state.painted > 0) {
    _out += `\x1b[${state.painted}A`;
  }
  for (let i = 0; i < lists.length; i++) {
    let _line = getStatusLine(lists[i]);
    /** Truncate to the terminal width: a wrapped line would break the in-place repaint */
    if (_line.length >= _columns) {
      _line = _line.slice(0, _columns - 2) + '…';
    }
    /** \x1b[K erases the end of the line: a shorter render leaves no characters behind */
    _out += '\r' + _line + '\x1b[K\n';
  }
  _out += '\x1b[?25h';
  process.stdout.write(_out);
  state.painted = lists.length;
}

function printStatusPlain (lists, state, force) {
  /** Without a TTY (CI, piped output) print only every RENDER_PERCENT_STEP percent of the round and the
   * final state. The progress comes from a counter: no scan of every queue on each element */
  const _percent = lists.total > 0 ? Math.round(lists.processed * 100 / lists.total) : 100;
  if (force !== true && state.percent !== null && _percent - state.percent < RENDER_PERCENT_STEP) {
    return;
  }
  state.percent = _percent;
  const _lines = [];
  /** Aggregated over the queues: one message is one block, so one flat set of fields */
  let _done = 0;
  let _total = 0;
  let _errors = 0;
  let _eta = 0;
  for (let i = 0; i < lists.length; i++) {
    const _queue = lists[i];
    _lines.push(getStatusLine(_queue));
    _done += _queue.time.done;
    _total += _queue.listLength;
    _errors += _queue.errors.length;
    /** The slowest queue decides when the round ends */
    if (_queue.done !== true && _queue.time.leftTime > _eta) {
      _eta = _queue.time.leftTime;
    }
  }
  /** Routed through log(): without a TTY the progress must reach the logger configured
   * with setLogFunction, and not only the stdout of the process */
  log(_lines.join('\n'), 'info', {
    event: 'queue_progress', queue: lists.queueName, try: lists.try, queues: lists.length,
    done: _done, total: _total, percent: _total > 0 ? Math.round(_done * 100 / _total) : 100, eta_ms: _eta, errors: _errors
  });
}

function eraseStatus () {
  if (activeStatus === null || process.stdout.isTTY !== true) {
    return;
  }
  if (activeStatus.state.painted > 0) {
    /** Move up and erase the painted block so a log message is printed above it */
    process.stdout.write(`\x1b[${activeStatus.state.painted}A\r\x1b[J`);
    activeStatus.state.painted = 0;
  }
}

function printQueueStatus(lists, force) {
  if (!lists || lists.length === 0 || lists[0]?.logQueueStatus === false) {
    return;
  }
  const _state = lists.renderState ?? (lists.renderState = { lastRenderNs: 0n, painted: 0, percent: null, finalized: false });
  if (_state.finalized === true) {
    return;
  }
  if (process.stdout.isTTY === true) {
    if (force !== true && _state.painted > 0 && Number(process.hrtime.bigint() - _state.lastRenderNs) * MS_PER_NS < RENDER_INTERVAL_MS) {
      return;
    }
    _state.lastRenderNs = process.hrtime.bigint();
    paintStatusTTY(lists, _state);
    activeStatus = { lists: lists, state: _state };
  } else {
    printStatusPlain(lists, _state, force);
  }
  if (force === true) {
    /** Final render: detach so later log messages are not printed above a finished block */
    _state.finalized = true;
    activeStatus = null;
  }
}

/**
 *
 * @description Create a new queue process
 *
 * @param {String} queueName Name used in logs and generated file names
 * @param {Array} list Array of elements to process (objects, strings, numbers...)
 * @param {Function} functionToExecute Worker `(element, next)` executed for each element
 * @param {Object} options [OPTIONAL] { concurrency, delay, retry, retryDelay, logEnabled, logQueueStatus, logDir, logRetentionDays, errorFile }
 * @param {Function} callback [OPTIONAL] `(err, results, errors)`, when omitted a promise resolving { results, errors } is returned
 */
async function execQueue (queueName, list, functionToExecute, options, callback) {
  if (typeof options === 'function' && !callback) {
    callback = options;
    options = null;
  }
  if (!callback) {
    /** Promise mode: without a callback the returned promise resolves with { results, errors } */
    callback = function (err, results, errors) {
      if (err) {
        throw (err instanceof Error ? err : new Error(err));
      }
      return { results: results, errors: errors };
    };
  }

  /** One clock read per round: the round duration costs nothing per element */
  const _roundStart = process.hrtime.bigint();
  let _queue;
  let _lists;
  /** Invalid arguments (a list that is not iterable, a logDir or a queue name that cannot become a string)
   * are returned through the callback, or as a rejection in promise mode, never as an unhandled rejection */
  try {
    if (options?.[NORMALIZED] !== true) {
      /** Options are cloned: the caller's object is never mutated and never leaks state between runs.
       * Retry rounds pass the internal object back and skip this step, avoiding a copy of the results */
      /** Precedence: the options of the call, then the ones of setDefaultOptions, then these defaults.
       * Internal keys are never read from the caller's options. An invalid number (NaN, 'abc', {},
       * negative, Infinity) falls back to the default instead of looping forever or dropping elements */
      options = {
        [NORMALIZED]  : true, // Internal - marks an already normalized options object
        concurrency   : Math.max(1, Math.floor(toFiniteOption(options?.concurrency ?? defaultOptions.concurrency, 1))), // Option - number of queues (at least 1)
        delay         : toFiniteOption(options?.delay ?? defaultOptions.delay, 0), // Option - MS delay between each execution
        retry         : Math.floor(toFiniteOption(options?.retry ?? defaultOptions.retry, 1)), // Option - Number of retries if an error is thrown
        logEnabled    : options?.logEnabled ?? defaultOptions.logEnabled ?? true, // Option - Log Start and End Performance summary, if false errors are still logged
        logQueueStatus: options?.logQueueStatus ?? defaultOptions.logQueueStatus ?? true, // Option - Log on the console each queue status and performances
        logDir        : path.resolve(options?.logDir ?? defaultOptions.logDir ?? path.join(process.cwd(), 'logs')), // Option - directory of the error/log files, resolved once so a later chdir cannot move it
        logRetentionDays: toFiniteOption(options?.logRetentionDays ?? defaultOptions.logRetentionDays, 0), // Option - delete the files of the queue older than X days, 0 keeps them all
        retryDelay    : toFiniteOption(Math.min(options?.retryDelay ?? defaultOptions.retryDelay ?? 0, RETRY_DELAY_MAX_MS), 0), // Option - MS to wait before the first retry round, doubled on each next round
        errorFile     : options?.errorFile ?? defaultOptions.errorFile ?? true, // Option - true: errors file on each failed round, 'final': only after the last retry, false: never
        try           : 0, // Internal - current retry attempt
        results       : [] // Internal - results accumulator across retries
      };
    }
    /** Computed once per round: a number is accepted as a name, and a CR / LF in it cannot forge a log line */
    _queue = toLogText(queueName);
    const _list = Array.isArray(list) ? list : [...list];
    if (options.logEnabled === true) {
      log(`[${_queue}] START - ${_list.length} total elements - ${options.concurrency} queue(s) - ${options.delay}ms delay - ${options.try}/${options.retry} retrie(s)`, 'info', {
        event: 'queue_start', queue: _queue, total: _list.length, concurrency: options.concurrency, delay: options.delay, retry: options.retry, try: options.try
      });
    }
    /** Create child-lists based on the concurrency option. Never more queues than elements: a huge
     * concurrency would only allocate empty queues */
    _lists = chunkify(_list, Math.min(options.concurrency, Math.max(1, _list.length)), options.logQueueStatus);
    /** Read by the status rendering, which only receives the lists */
    _lists.queueName = _queue;
    _lists.try = options.try;
    /** Counters: neither the end of a queue nor the status rendering scans every queue */
    _lists.finished = 0;
    _lists.processed = 0;
    _lists.total = _list.length;
  } catch (err) {
    return callback(err);
  }
  /** Create an array of promises, each promise is a queue */
  const _listPromises = []
  _lists.forEach((el, index) => {
    _listPromises.push(unpackGenericQueuePromisify(index, _lists, functionToExecute, options.delay))
  })
  /** The outcome is decided inside the try block, the callback is called once after it: a callback
   * that throws is never caught and called a second time with an internal error */
  let _remaining = [];
  let _crash = null;
  try {
    /** Execute all queues in parrallel, end only when all queues are done */
    const _res = await Promise.allSettled(_listPromises);
    /** Single pass aggregation instead of one map+flat+filter per collection */
    const _errors = [];
    const _results = [];
    const _logs = [];
    for (let i = 0; i < _res.length; i++) {
      const _value = _res[i]?.value;
      if (!_value) {
        continue;
      }
      for (let j = 0; j < _value.errors.length; j++) {
        _errors.push(_value.errors[j]);
      }
      for (let j = 0; j < _value.results.length; j++) {
        _results.push(_value.results[j]);
      }
      for (let j = 0; j < _value.logs.length; j++) {
        _logs.push(_value.logs[j]);
      }
    }
    /** A loop instead of [...spread, ...spread]: no full copy of the accumulated results on each round */
    for (let i = 0; i < _results.length; i++) {
      options.results.push(_results[i]);
    }
    if (_logs.length > 0) {
      /** Awaited: a caller exiting right after the callback must not truncate the file */
      await createLogFile(_queue, _logs, 'logs', options);
    }
    if (_errors.length > 0) {
      /** The distinct messages are logged inline: the failure is diagnosable from the log
       * alone, without opening the error file */
      const _summary = getErrorSummary(_errors);
      log(`[${_queue}] ${_errors.length} errors: ${_summary}`, 'error', {
        event: 'queue_errors', queue: _queue, errors: _errors.length, try: options.try, summary: _summary
      });
      const _isLastRound = options.try >= options.retry;
      /** errorFile: false never writes the file, 'final' only for the errors returned to the caller */
      const _writeFile = options.errorFile !== false && (options.errorFile !== 'final' || _isLastRound);
      /** Only a file actually written is pointed to by the END line */
      const _fileWritten = _writeFile === true && await createLogFile(_queue, _errors, 'errors', options, 'error');
      if (_isLastRound === false) {
        options.try += 1;
        const _toRetry = _errors.map(value => value.element);
        const _delay = getRetryDelay(options.retryDelay, options.try);
        /** A retry is a warning: the queue can still recover on the next round */
        log(`[${_queue}] Retry to re-execute the process on failled elements...`, 'warn', {
          event: 'queue_retry', queue: _queue, try: options.try, elements: _toRetry.length, delay_ms: _delay
        });
        /** A regular (ref'd) timer: the pending retry round is work the caller waits for, so it keeps
         * the process alive exactly like an in-flight element would. An unref'd timer could let the
         * process exit silently before the round, without ever calling back.
         * Without a delay the round still yields to the event loop: a worker failing synchronously
         * never chains the rounds in a pure microtask loop, which would starve timers and I/O */
        await new Promise(resolve => (_delay > 0 ? setTimeout(resolve, _delay) : setImmediate(resolve)));
        return execQueue(queueName, _toRetry, functionToExecute, options, callback);
      } else {
        log(`[${_queue}] END - Stop retrying${_fileWritten === true ? ', check the error file!' : ''}`, 'error', {
          event: 'queue_failed', queue: _queue, errors: _errors.length, try: options.try, total: list.length, duration_ms: elapsedMs(_roundStart)
        });
        /** Errors remaining after the last retry are passed to the callback */
        _remaining = _errors;
      }
    } else {
      if (options.logEnabled === true) {
        const _slowest = getSlowestQueue(_lists);
        log(`[${_queue}] END - ${getPerfSummary(_lists, _results.length, _errors.length, _logs.length)}`, 'info', {
          event: 'queue_end', queue: _queue, total: list.length, results: options.results.length, errors: 0, try: options.try,
          duration_ms: elapsedMs(_roundStart), avg_ms: _slowest ? _slowest.time.averageTime : 0
        });
      }
    }
  } catch (err) {
    const _error = toLogText(err);
    _crash = `[${_queue}] Error: Promise All Catched: ${_error}`;
    log(_crash, 'error', { event: 'queue_crash', queue: _queue, try: options.try, error: _error });
  }
  if (_crash !== null) {
    return callback(_crash);
  }
  return callback(null, options.results, _remaining);
}

/**
 * Wait before a retry round: `retryDelay * 2^(try - 1)`, capped to RETRY_DELAY_MAX_MS
 *
 * @param {Number} retryDelay Option, ms before the first retry round
 * @param {Number} nextTry Number of the retry round about to start, from 1
 * @returns {Number} Milliseconds, 0 for a missing, negative or invalid option
 */
function getRetryDelay (retryDelay, nextTry) {
  if (!(retryDelay > 0)) {
    return 0;
  }
  /** An Infinity coming from a huge option or exponent is capped as well */
  return Math.round(Math.min(retryDelay * 2 ** (nextTry - 1), RETRY_DELAY_MAX_MS));
}

/**
 * Normalize a numeric option
 *
 * @param {*} value Value of the option
 * @param {Number} fallback Returned for anything else than a finite number >= 0
 * @returns {Number}
 */
function toFiniteOption (value, fallback) {
  const _number = Number(value ?? fallback);
  return Number.isFinite(_number) && _number >= 0 ? _number : fallback;
}

function elapsedMs (startNs) {
  return Math.round(Number(process.hrtime.bigint() - startNs) * MS_PER_NS);
}

/**
 * Summarize a list of errors as their distinct messages with their number of occurrences,
 * most frequent first, capped to ERROR_SUMMARY_MAX_ENTRIES entries followed by "+N more"
 *
 * @param {Array} errors Array of { element, message }
 * @returns {String} ex: `"ECONNREFUSED" x9, "ETIMEDOUT" x2, +1 more`, always a single line
 */
function getErrorSummary (errors) {
  const _counts = new Map();
  for (let i = 0; i < errors.length; i++) {
    /** Grouped on the full message: two long messages differing at the end stay distinct */
    const _message = errors[i]?.message ? String(errors[i].message) : 'Unknown error';
    _counts.set(_message, (_counts.get(_message) ?? 0) + 1);
  }
  /** Most frequent first, the insertion order is kept between equal counts (stable sort) */
  const _sorted = [..._counts.entries()].sort((a, b) => b[1] - a[1]);
  const _summary = [];
  for (let i = 0; i < _sorted.length && i < ERROR_SUMMARY_MAX_ENTRIES; i++) {
    /** Only the displayed messages are cleaned (3 at most): a message of a worker can contain a CR / LF */
    let _message = toLogText(_sorted[i][0]);
    if (_message.length > ERROR_SUMMARY_MAX_MESSAGE_LENGTH) {
      _message = _message.slice(0, ERROR_SUMMARY_MAX_MESSAGE_LENGTH - 1) + '…';
    }
    /** JSON quoting escapes a `"` or a `\` of the message: the closing quote is always the real one */
    _summary.push(`${JSON.stringify(_message)} x${_sorted[i][1]}`);
  }
  if (_sorted.length > _summary.length) {
    /** The count of distinct messages left out, not of remaining errors */
    _summary.push(`+${_sorted.length - _summary.length} more`);
  }
  return _summary.join(', ');
}

function getQueueSlug (queue) {
  /** Whitelist: the queue name is part of a file name. A path separator, a null byte or a
   * `..` segment would let a name coming from untrusted data escape the log directory */
  return queue.replace(/\s/g, '-').toLowerCase().replace(/[^a-z0-9._-]/g, '-').slice(0, QUEUE_SLUG_MAX_LENGTH);
}

function escapeRegExp (str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Write the errors or the logs of a queue as a JSON file
 *
 * @param {String} queue Name of the queue, already a single-line string (toLogText)
 * @param {Array} content Elements to serialize
 * @param {String} label `errors` or `logs`, part of the file name
 * @param {Object} options Normalized options (try, logDir, logRetentionDays)
 * @param {String} level [OPTIONAL] Level used to log the path of the created file
 * @returns {Promise<Boolean>} Resolved once the file is fully written and the retention applied, with
 *                    true when the file was written. Never rejected: a write failure is logged and the
 *                    queue results are kept
 */
function createLogFile (queue, content, label, options, level = 'info') {
  /** Second precision + retry attempt suffix: a retry executed in the same minute does not overwrite the previous file */
  const _suffix = options.try > 0 ? `-try${options.try}` : '';
  const _basename = new Date().toISOString().replace(/:/g, '-').slice(0, 19) + `-${getQueueSlug(queue)}${label ? '-' + label : ''}${_suffix}`;
  let _filename = _basename + '.json';
  let _path = path.join(options.logDir, _filename);
  /** Same fields as the log_file event plus the message of the failure, logged at error */
  const _fileError = (err) => ({ event: 'log_file_error', queue: queue, label: label, path: _path, error: toLogText(err) });

  return new Promise(function (resolve) {
    /** Second guard, in case the name sanitizing ever lets something through: the file is
     * always written directly inside the log directory, never in a parent or a sub folder */
    if (path.dirname(_path) !== options.logDir) {
      log(`[${queue}] Error Create Log File: ${_path} is outside of ${options.logDir}`, 'error', _fileError(`outside of ${options.logDir}`));
      return resolve(false);
    }
    try {
      /** The files contain the raw elements (emails, tokens...): a new directory is private to the owner,
       * an existing one is left as it is */
      fs.mkdirSync(options.logDir, { recursive: true, mode: 0o700 });
    } catch (err) {
      log(`[${queue}] Error Create Log Folder: ${toLogText(err)}`, 'error', _fileError(err));
      return resolve(false);
    }
    openFile(false);

    function openFile (isRetry) {
      /** 'wx' fails instead of following a symlink or overwriting a file planted in the log directory.
       * 0o600: the raw elements of the file are readable by the owner only */
      fs.open(_path, 'wx', 0o600, (err, fd) => {
        if (err?.code === 'EEXIST' && isRetry === false) {
          /** Same queue, label and second as another run: retried once under a unique name, still with 'wx' */
          _filename = `${_basename}-${process.pid}-${++logFileCollisions}.json`;
          _path = path.join(options.logDir, _filename);
          return openFile(true);
        }
        if (err) {
          log(`[${queue}] Error Create Log File: ${toLogText(err)}`, 'error', _fileError(err));
          return resolve(false);
        }
        writeFile(fs.createWriteStream(null, { fd: fd }));
      });
    }

    function writeFile (stream) {
      /** The queue awaits this promise: it must settle exactly once, on 'finish' as on 'error' */
      let _settled = false;
      stream.on('error', (err) => {
        log(`[${queue}] Error Create Log File: ${toLogText(err)}`, 'error', _fileError(err));
        if (_settled === false) {
          _settled = true;
          return resolve(false);
        }
      });
      stream.on('finish', () => {
        if (_settled === false) {
          _settled = true;
          /** Announced once written: the path is logged at the level of its content, an error file must not be stranded at info */
          log(`[${queue}] Created ${label ? label + ' ' : ''}file: ${_path}`, level, { event: 'log_file', queue: queue, label: label, path: _path });
          return removeOldLogFiles(queue, label, options, _filename, () => resolve(true));
        }
      });
      let _index = 0;
      /** The content is serialized chunk by chunk with backpressure: one JSON.stringify of a
       * huge errors/logs array would block the event loop and buffer the whole file in memory */
      function writeNextChunk () {
        /** A failed write (disk full...) destroys the stream: the rest of the content is not serialized */
        if (stream.destroyed === true) {
          return;
        }
        let _buffer = _index === 0 ? '[' : '';
        const _end = Math.min(_index + LOG_FILE_CHUNK_SIZE, content.length);
        for (; _index < _end; _index++) {
          let _json;
          /** A circular structure or a BigInt makes JSON.stringify throw: serialized as null,
           * like any other non-serializable entry, instead of crashing from a setImmediate */
          try {
            _json = JSON.stringify(content[_index]);
          } catch (err) {
            _json = 'null';
          }
          _buffer += (_json ?? 'null') + (_index + 1 < content.length ? ',' : '');
        }
        if (_index >= content.length) {
          return stream.end(_buffer + ']');
        }
        if (stream.write(_buffer) === false) {
          return stream.once('drain', writeNextChunk);
        }
        return setImmediate(writeNextChunk);
      }
      writeNextChunk();
    }
  });
}

/**
 * Delete the files of the same queue and label older than `options.logRetentionDays`
 * A cleanup failure is not a queue failure: it is logged as a warning and never blocks the callback
 *
 * @param {String} queue Name of the queue, already a single-line string (toLogText)
 * @param {String} label `errors` or `logs`
 * @param {Object} options Normalized options (logDir, logRetentionDays)
 * @param {String} currentFile Name of the file just written, never deleted
 * @param {Function} callback Called once the cleanup is done
 */
function removeOldLogFiles (queue, label, options, currentFile, callback) {
  if (!(options.logRetentionDays > 0)) {
    return callback();
  }
  const _limit = Date.now() - (options.logRetentionDays * MS_PER_DAY);
  fs.readdir(options.logDir, (err, files) => {
    if (err) {
      log(`[${queue}] Error Read Log Folder: ${toLogText(err)}`, 'warn', {
        event: 'log_cleanup_error', queue: queue, label: label, path: options.logDir, error: toLogText(err)
      });
      return callback();
    }
    /** Only the files of this queue and label: another queue, or another program writing
     * in the same folder, is never touched */
    /** `-<pid>-<n>` is the unique suffix of a file created after a same-second collision */
    const _pattern = new RegExp(`^(.{19})-${escapeRegExp(getQueueSlug(queue))}${label ? '-' + label : ''}(?:-try\\d+)?(?:-\\d+-\\d+)?\\.json$`);
    const _toRemove = [];
    for (let i = 0; i < files.length; i++) {
      const _match = _pattern.exec(files[i]);
      /** The date comes from the file name: no fs.stat on every file of the folder.
       * An unreadable date gives NaN, which compares false and keeps the file */
      if (_match !== null && files[i] !== currentFile && parseLogFileDate(_match[1]) < _limit) {
        _toRemove.push(files[i]);
      }
    }
    if (_toRemove.length === 0) {
      return callback();
    }
    let _pending = _toRemove.length;
    for (let i = 0; i < _toRemove.length; i++) {
      fs.unlink(path.join(options.logDir, _toRemove[i]), (err) => {
        if (err) {
          log(`[${queue}] Error Remove Log File: ${toLogText(err)}`, 'warn', {
            event: 'log_cleanup_error', queue: queue, label: label, path: path.join(options.logDir, _toRemove[i]), error: toLogText(err)
          });
        }
        _pending -= 1;
        if (_pending === 0) {
          return callback();
        }
      });
    }
  });
}

/**
 * Convert the `2026-08-12T14-05-33` prefix of a file name back to a timestamp
 *
 * @param {String} stamp Date part of the file name
 * @returns {Number} Milliseconds, NaN when the prefix is not a date written by createLogFile
 */
function parseLogFileDate (stamp) {
  return Date.parse(stamp.slice(0, 11) + stamp.slice(11).replace(/-/g, ':') + 'Z');
}

function chunkify(list, size, logQueueStatus) {
  let result = [];
  let array = [...list];
  /** A moving start index instead of array.splice(0, n): splicing the head shifts the rest of the
   * array each time, O(n) per queue, O(n²) when there are as many queues as elements */
  let _start = 0;
  for (let i = size; i > 0; i--) {
    const _end = _start + Math.ceil((array.length - _start) / i);
    const _chunkList = array.slice(_start, _end);
    _start = _end;
    result.push(
      {
        id  : result.length,
        time: {
          requestTime: 0,
          averageTime: 0,
          leftTime   : 0,
          passedTime : 0,
          percentage : 0,
          done       : 0
        },
        list          : _chunkList,
        listLength    : _chunkList.length,
        cursor        : 0,
        done          : false,
        logQueueStatus: logQueueStatus ?? true,
        errors        : [],
        results       : [],
        logs          : []
      }
    );
  }
  return result;
}

function getSlowestQueue (lists) {
  return lists.reduce(function(prev, current) {
    return (prev.time.passedTime > current.time.passedTime) ? prev : current
  });
}

function getPerfSummary(lists, resultsLength, errorsLength, logsLength) {
  // Choose the slowest queue to print the log
  let _slowest = getSlowestQueue(lists);
  if (_slowest) {
    return `Duration: ${msToTime(_slowest.time.passedTime)} | Avg time/exec: ${msToTime(_slowest.time.averageTime)} | Errors: ${errorsLength} | Returned: ${resultsLength} | Logs: ${logsLength}`
  } else {
    return `Error get performances summary`
  }
}

function ignoreError () {}

/** Output function, replaceable with setLogFunction */
let logOutput = function (msg, level = 'info') {
  /** The level is prefixed as-is: the severity stays visible on a bare console */
  return console.log(level === 'info' ? msg : `${level.toUpperCase()} ${msg}`);
}

/**
 * log messages above the live status block
 *
 * @param {String} msg Message
 * @param {String} level info, warn, error
 * @param {Object} fields Flat object of strings, numbers and booleans, always with `event` and `queue`
 */
function log (msg, level = 'info', fields) {
  /** An active TTY status block is erased, the message printed, and the block repainted below it */
  eraseStatus();
  let _res;
  try {
    _res = logOutput(msg, level, fields);
    /** Same for an async logger: its rejected promise must not become an unhandledRejection */
    if (typeof _res?.then === 'function') {
      _res.then(undefined, ignoreError);
    }
  } catch (err) {
    /** A failing host logger must never crash the queue nor leave its callback uncalled */
  }
  if (activeStatus !== null && process.stdout.isTTY === true) {
    paintStatusTTY(activeStatus.lists, activeStatus.state);
  }
  return _res;
}

/**
 * Define the options applied to every execQueue call
 * The options passed to execQueue always win, and a new call replaces the previous defaults
 *
 * @param {Object} newOptions { concurrency, delay, retry, retryDelay, logEnabled, logQueueStatus, logDir, logRetentionDays, errorFile }
 *                            null or nothing resets them
 */
function setDefaultOptions (newOptions) {
  /** Copied: a later mutation of the caller's object never changes the behaviour of a running queue */
  defaultOptions = newOptions ? { ...newOptions } : {};
}

/**
 * Replace the default logger
 *
 * @param {Function} newLogFunction `(message, level, fields)`: `fields` is a flat object of
 *                                  strings, numbers and booleans, always with `event` and `queue`
 */
function setLogFunction (newLogFunction) {
  /** Anything else than a function is ignored: the current logger is kept */
  if (typeof newLogFunction === 'function') {
    logOutput = newLogFunction;
  }
}

module.exports = {
  msToTime,
  execQueue,
  chunkify,
  setLogFunction,
  setDefaultOptions,
  NS_PER_SEC,
  MS_PER_NS
}
