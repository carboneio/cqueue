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
/** Number of distinct error messages detailed in the error summary line */
const ERROR_SUMMARY_MAX_ENTRIES = 3;
/** Each message of the error summary is truncated to X characters to keep the line readable */
const ERROR_SUMMARY_MAX_MESSAGE_LENGTH = 100;

/** Status block currently painted on the TTY, erased/repainted around log messages */
let activeStatus = null;

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
    let _allDone = true;
    for (let i = 0; i < lists.length; i++) {
      if (lists[i].done !== true) {
        _allDone = false;
        break;
      }
    }
    /** The very last queue forces a final render so the status always shows the completed state */
    printQueueStatus(lists, _allDone);
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
        _handled = true;
        if (err) {
          _queue.errors.push({
            element: _element,
            message: err.toString()
          });
        }
        /** Keep any provided result, including falsy values such as 0, '' or false */
        if (res !== undefined && res !== null) {
          _queue.results.push(res);
        }
        const _logs = actions?.logs instanceof Array ? actions.logs : [];
        /** A loop instead of push(...spread): a spread of a huge array can overflow the call stack */
        for (let i = 0; i < _logs.length; i++) {
          _queue.logs.push(_logs[i]);
        }
        if (actions?.stop === true) {
          /** Stop ALL queues: the shared flag is checked by every queue before executing its next element */
          lists.stopped = true;
        } else {
          _queue.cursor += 1;
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
  /** Without a TTY (CI, piped output) print only every RENDER_PERCENT_STEP percent and the final state */
  let _shouldPrint = force === true || state.percents === null;
  if (_shouldPrint === false) {
    for (let i = 0; i < lists.length; i++) {
      if (lists[i].time.percentage - state.percents[i] >= RENDER_PERCENT_STEP) {
        _shouldPrint = true;
        break;
      }
    }
  }
  if (_shouldPrint === false) {
    return;
  }
  const _lines = [];
  state.percents = [];
  for (let i = 0; i < lists.length; i++) {
    state.percents.push(lists[i].time.percentage);
    _lines.push(getStatusLine(lists[i]));
  }
  /** Routed through log(): without a TTY the progress must reach the logger configured
   * with setLogFunction, and not only the stdout of the process */
  log(_lines.join('\n'), 'info');
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
  const _state = lists.renderState ?? (lists.renderState = { lastRenderNs: 0n, painted: 0, percents: null, finalized: false });
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
 * @param {Object} options [OPTIONAL] { concurrency, delay, retry, logEnabled, logQueueStatus }
 * @param {Function} callback [OPTIONAL] `(err, results, errors)`, when omitted a promise resolving { results, errors } is returned
 */
async function execQueue (queueName, list, functionToExecute, options, callback) {
  if (typeof options === 'function' && !callback) {
    callback = options;
    options = null;
  }
  if (options?._cqueue !== true) {
    /** Options are cloned: the caller's object is never mutated and never leaks state between runs.
     * Retry rounds pass the internal object back and skip this step, avoiding a copy of the results */
    options = {
      _cqueue       : true, // Internal - marks an already normalized options object
      concurrency   : options?.concurrency ?? 1, // Option - number of queues
      delay         : options?.delay ?? 0, // Option - MS delay between each execution
      retry         : options?.retry ?? 1, // Option - Number of retries if an error is thrown
      logEnabled    : options?.logEnabled ?? true, // Option - Log Start and End Performance summary, if false errors are still logged
      logQueueStatus: options?.logQueueStatus ?? true, // Option - Log on the console each queue status and performances
      logDir        : path.resolve(options?.logDir ?? path.join(process.cwd(), 'logs')), // Option - directory of the error/log files, resolved once so a later chdir cannot move it
      logRetention  : options?.logRetention ?? 0, // Option - max number of files kept per queue name and label, 0 keeps them all
      try           : options?.try ?? 0, // Internal - current retry attempt
      results       : options?.results ? [...options.results] : [] // Internal - results accumulator across retries
    };
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

  if (options.logEnabled === true) {
    log(`[${queueName}] START - ${list.length} total elements - ${options.concurrency} queue(s) - ${options.delay}ms delay - ${options.try}/${options.retry} retrie(s)`);
  }
  /** Create child-lists based on the concurrency option */
  const _lists = chunkify(list, options.concurrency, options.logQueueStatus);
  /** Create an array of promises, each promise is a queue */
  const _listPromises = []
  _lists.forEach((el, index) => {
    _listPromises.push(unpackGenericQueuePromisify(index, _lists, functionToExecute, options.delay))
  })
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
      await createLogFile(queueName, _logs, 'logs', options);
    }
    if (_errors.length > 0) {
      /** The distinct messages are logged inline: the failure is diagnosable from the log
       * alone, without opening the error file */
      log(`[${queueName}] ${_errors.length} errors: ${getErrorSummary(_errors)}`, 'error')
      await createLogFile(queueName, _errors, 'errors', options, 'error');
      const _toRetry = _errors.map(value => value.element);
      if (options.try < options.retry) {
        options.try += 1;
        /** A retry is a warning: the queue can still recover on the next round */
        log(`[${queueName}] Retry to re-execute the process on failled elements...`, 'warn')
        return execQueue(queueName, _toRetry, functionToExecute, options, callback);
      } else {
        log(`[${queueName}] END - Stop retrying, check the error file!`, 'error')
        /** Errors remaining after the last retry are passed to the callback */
        return callback(null, options.results, _errors);
      }
    } else {
      if (options.logEnabled === true) {
        log(`[${queueName}] END - ${getPerfSummary(_lists, _results.length, _errors.length, _logs.length)}`)
      }
    }
  } catch (err) {
    log(`[${queueName}] Error: Promise All Catched: ${err.toString()}`, 'error');
    return callback(`[${queueName}] Error: Promise All Catched: ${err.toString()}`);
  }
  return callback(null, options.results, []);
}

/**
 * Summarize a list of errors as their distinct messages with their number of occurrences,
 * most frequent first, capped to ERROR_SUMMARY_MAX_ENTRIES entries followed by "+N more"
 *
 * @param {Array} errors Array of { element, message }
 * @returns {String} ex: `"ECONNREFUSED" x9, "ETIMEDOUT" x2, +1 more`
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
    let _message = _sorted[i][0];
    if (_message.length > ERROR_SUMMARY_MAX_MESSAGE_LENGTH) {
      _message = _message.slice(0, ERROR_SUMMARY_MAX_MESSAGE_LENGTH - 1) + '…';
    }
    _summary.push(`"${_message}" x${_sorted[i][1]}`);
  }
  if (_sorted.length > _summary.length) {
    /** The count of distinct messages left out, not of remaining errors */
    _summary.push(`+${_sorted.length - _summary.length} more`);
  }
  return _summary.join(', ');
}

function getQueueSlug (queueName) {
  /** Whitelist: the queue name is part of a file name. A path separator, a null byte or a
   * `..` segment would let a name coming from untrusted data escape the log directory */
  return queueName.replace(/\s/g, '-').toLowerCase().replace(/[^a-z0-9._-]/g, '-');
}

function escapeRegExp (str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Write the errors or the logs of a queue as a JSON file
 *
 * @param {String} queueName Name of the queue
 * @param {Array} content Elements to serialize
 * @param {String} label `errors` or `logs`, part of the file name
 * @param {Object} options Normalized options (try, logDir, logRetention)
 * @param {String} level [OPTIONAL] Level used to log the path of the created file
 * @returns {Promise} Resolved once the file is fully written and the retention applied.
 *                    Never rejected: a write failure is logged and the queue results are kept
 */
function createLogFile (queueName, content, label, options, level = 'info') {
  /** Second precision + retry attempt suffix: a retry executed in the same minute does not overwrite the previous file */
  const _suffix = options.try > 0 ? `-try${options.try}` : '';
  const _filename = new Date().toISOString().replace(/:/g, '-').slice(0, 19) + `-${getQueueSlug(queueName)}${label ? '-' + label : ''}${_suffix}.json`
  const _path = path.join(options.logDir, _filename);
  /** The path is logged at the level of its content: an error file must not be stranded at info */
  log(`[${queueName}] Created ${label ? label + ' ' : ''}file: ${_path}`, level);

  return new Promise(function (resolve) {
    /** Second guard, in case the name sanitizing ever lets something through: the file is
     * always written directly inside the log directory, never in a parent or a sub folder */
    if (path.dirname(_path) !== options.logDir) {
      log(`[${queueName}] Error Create Log File: ${_path} is outside of ${options.logDir}`, 'error');
      return resolve();
    }
    try {
      fs.mkdirSync(options.logDir, { recursive: true });
    } catch (err) {
      log(`[${queueName}] Error Create Log Folder: ${err.toString()}`, 'error');
      return resolve();
    }
    /** 'wx' fails instead of following a symlink or overwriting a file planted in the log
     * directory, and turns a same-second collision into an error instead of a corrupted file */
    const _stream = fs.createWriteStream(_path, { flags: 'wx' });
    /** The queue awaits this promise: it must settle exactly once, on 'finish' as on 'error' */
    let _settled = false;
    _stream.on('error', (err) => {
      log(`[${queueName}] Error Create Log File: ${err.toString()}`, 'error');
      if (_settled === false) {
        _settled = true;
        return resolve();
      }
    });
    _stream.on('finish', () => {
      if (_settled === false) {
        _settled = true;
        return removeOldLogFiles(queueName, label, options, resolve);
      }
    });
    let _index = 0;
    /** The content is serialized chunk by chunk with backpressure: one JSON.stringify of a
     * huge errors/logs array would block the event loop and buffer the whole file in memory */
    function writeNextChunk () {
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
        return _stream.end(_buffer + ']');
      }
      if (_stream.write(_buffer) === false) {
        return _stream.once('drain', writeNextChunk);
      }
      return setImmediate(writeNextChunk);
    }
    writeNextChunk();
  });
}

/**
 * Delete the oldest files of the same queue and label, keeping `options.logRetention` of them
 * A cleanup failure is not a queue failure: it is logged as a warning and never blocks the callback
 *
 * @param {String} queueName Name of the queue
 * @param {String} label `errors` or `logs`
 * @param {Object} options Normalized options (logDir, logRetention)
 * @param {Function} callback Called once the cleanup is done
 */
function removeOldLogFiles (queueName, label, options, callback) {
  if (!(options.logRetention > 0)) {
    return callback();
  }
  fs.readdir(options.logDir, (err, files) => {
    if (err) {
      log(`[${queueName}] Error Read Log Folder: ${err.toString()}`, 'warn');
      return callback();
    }
    /** Only the files of this queue and label: another queue, or another program writing
     * in the same folder, is never touched */
    const _pattern = new RegExp(`^(.{19})-${escapeRegExp(getQueueSlug(queueName))}${label ? '-' + label : ''}(?:-try(\\d+))?\\.json$`);
    const _files = [];
    for (let i = 0; i < files.length; i++) {
      const _match = _pattern.exec(files[i]);
      if (_match !== null) {
        _files.push({ name: files[i], date: _match[1], try: _match[2] !== undefined ? Number(_match[2]) : 0 });
      }
    }
    if (_files.length <= options.logRetention) {
      return callback();
    }
    /** Oldest first: the ISO timestamp prefix sorts lexicographically and the retry number breaks
     * the ties of rounds written within the same second */
    _files.sort((a, b) => a.date < b.date ? -1 : a.date > b.date ? 1 : a.try - b.try);
    const _toRemove = _files.slice(0, _files.length - options.logRetention);
    let _pending = _toRemove.length;
    for (let i = 0; i < _toRemove.length; i++) {
      fs.unlink(path.join(options.logDir, _toRemove[i].name), (err) => {
        if (err) {
          log(`[${queueName}] Error Remove Log File: ${err.toString()}`, 'warn');
        }
        _pending -= 1;
        if (_pending === 0) {
          return callback();
        }
      });
    }
  });
}

function chunkify(list, size, logQueueStatus) {
  let result = [];
  let array = [...list];
  for (let i = size; i > 0; i--) {
    const _chunkList = array.splice(0, Math.ceil(array.length / i));
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

function getPerfSummary(lists, resultsLength, errorsLength, logsLength) {
  // Choose the slowest queue to print the log
  let _slowest = lists.reduce(function(prev, current) {
    return (prev.time.passedTime > current.time.passedTime) ? prev : current
  })
  if (_slowest) {
    return `Duration: ${msToTime(_slowest.time.passedTime)} | Avg time/exec: ${msToTime(_slowest.time.averageTime)} | Errors: ${errorsLength} | Returned: ${resultsLength} | Logs: ${logsLength}`
  } else {
    return `Error get performances summary`
  }
}

/** Output function, replaceable with setLogFunction */
let logOutput = function (msg, level = 'info') {
  /** The level is prefixed as-is: the severity stays visible on a bare console */
  return console.log(level === 'info' ? msg : `${level.toUpperCase()} ${msg}`);
}

/**
 * log messages above the live status block
 *
 * @param {String} msg Message
 * @param {type} level warning, error
 */
function log (msg, level = 'info') {
  /** An active TTY status block is erased, the message printed, and the block repainted below it */
  eraseStatus();
  const _res = logOutput(msg, level);
  if (activeStatus !== null && process.stdout.isTTY === true) {
    paintStatusTTY(activeStatus.lists, activeStatus.state);
  }
  return _res;
}

function setLogFunction (newLogFunction) {
  if (newLogFunction) {
    logOutput = newLogFunction;
  }
}

module.exports = {
  msToTime,
  execQueue,
  chunkify,
  setLogFunction,
  NS_PER_SEC,
  MS_PER_NS
}
