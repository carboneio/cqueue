# cqueue

Zero-dependency concurrent queue executor for Node.js. Split a list of elements into N parallel queues, execute an asynchronous function on each element, and get aggregated results, errors, automatic retries, live progress and performance stats.

## Features

- 🚀 **Concurrency**: split the work across N parallel queues
- 🔁 **Automatic retries**: failed elements are re-executed, optionally after an exponential backoff, remaining errors are returned
- ⏱ **Delay**: optional pause between each execution (rate limiting)
- 🛑 **Stop action**: any element can stop all queues
- 📊 **Live progress**: per-queue percentage, average time per execution, estimated time left
- 📄 **Error / log files**: failures and custom logs are written as JSON files
- 🧾 **Structured logs**: every message comes with flat fields (`event`, `queue`, counts, durations) ready for logfmt / JSON loggers
- 🪶 **Zero dependency**, callback and promise friendly

## Install

```bash
npm install @carboneio/cqueue
```

## Quick start

```js
const { execQueue } = require('@carboneio/cqueue');

const files = ['a.pdf', 'b.pdf', 'c.pdf' /* ... thousands more */];

execQueue('convert-files', files, (file, next) => {
  convert(file, (err, output) => {
    if (err) {
      return next(err); // recorded as an error, retried automatically
    }
    return next(null, output); // recorded as a result
  });
}, { concurrency: 10 }, (err, results, errors) => {
  console.log(`${results.length} converted, ${errors.length} failed after retries`);
});
```

Or with `async/await` (omit the callback):

```js
const { results, errors } = await execQueue('convert-files', files, worker, { concurrency: 10 });
```

## API

### `execQueue(queueName, list, functionToExecute [, options] [, callback])`

| Argument | Type | Description |
|---|---|---|
| `queueName` | `String` | Name used in logs and generated file names (a number is accepted). Reduced to `[a-z0-9._-]` in file names, so a name built from untrusted data cannot escape `logDir`, and its control characters are replaced by a space in log messages |
| `list` | `Array` | Elements to process (objects, strings, numbers…) |
| `functionToExecute` | `Function` | Worker `(element, next) => {}` executed for each element |
| `options` | `Object` | Optional, see below. Merged over the ones of [`setDefaultOptions`](#setdefaultoptionsoptions) |
| `callback` | `Function` | Optional `(err, results, errors) => {}`. When omitted, a promise resolving `{ results, errors }` is returned |

#### Options

| Option | Type | Default | Description |
|---|---|---|---|
| `concurrency` | `Number` | `1` | Number of parallel queues. The list is split into `concurrency` balanced chunks, each processed sequentially, and never into more chunks than elements |
| `delay` | `Number` | `0` | Milliseconds to wait between each execution in a queue (rate limiting). With `0`, elements completing synchronously are executed in batches that yield back to the event loop every few milliseconds: no artificial delay, no event loop starvation |
| `retry` | `Number` | `1` | Number of extra rounds re-executing only the failed elements. `0` disables retries |
| `retryDelay` | `Number` | `0` | Milliseconds to wait before each retry round, doubled on every round: `retryDelay * 2 ** (try - 1)`, capped to 60 seconds. `0` (or a negative / invalid value) starts the retry round immediately, like before. See [Retry delay](#retry-delay) |
| `logEnabled` | `Boolean` | `true` | Log the START line and the END performance summary. Errors are still logged when `false` |
| `logDir` | `String` | `<cwd>/logs` | Directory of the generated error / log JSON files, created if missing (mode `0700`; an existing directory is left as it is). The files contain your raw elements and are created with mode `0600`. Set it to an absolute path so the files do not depend on the directory the process was started from |
| `errorFile` | `Boolean \| 'final'` | `true` | `true` writes an errors JSON file after each failed round, `'final'` only after the last round (the errors returned to the caller), `false` never. The error summary is logged in every case. Any other value behaves as `true` |
| `logRetentionDays` | `Number` | `0` | Delete the error / log files of that queue older than X days, after each successful write. Fractions are accepted (`0.5` = 12 hours). `0` keeps every file. The age comes from the timestamp in the file name, and only the files of that queue name and label are ever deleted — never the file of the current run, nor anything else in the directory. File names use the queue name reduced to `[a-z0-9._-]` and 200 characters, so queues whose names give the same reduced name (`Billing/Prod` and `billing prod`) share their files and their retention: give them distinct names, or distinct `logDir` |
| `logQueueStatus` | `Boolean` | `true` | Live per-queue progress (`[0] 45% - 45/100 - Passed time: 2.1 Sec \| Left Time: 2.5 Sec \| Avg time/exec: 47 ms`). See [Live status rendering](#live-status-rendering) |

A numeric option that is not a finite number `>= 0` (`NaN`, `'abc'`, `{}`, a negative value, `Infinity`) falls back to its default; `concurrency` and `retry` are rounded down. `retryDelay: Infinity` is the exception: it is capped to 60 seconds.

#### Callback / promise result

- `err`: internal execution error (`null` in normal operation, even when elements fail — element failures are reported through `errors`)
- `results`: every non-`null`/`undefined` value passed as `next(null, result)`, aggregated across all queues and retry rounds. Falsy values such as `0`, `''` and `false` are kept
- `errors`: elements still failing after the last retry round, as `[{ element, message }]`

The callback is called exactly once. If it throws, the exception is not caught: it rejects the promise returned by `execQueue` (an unhandled rejection when nobody awaits it, like any throw in an async callback).

### The worker function: `(element, next)`

Call `next(err, result, actions)` exactly once per element (extra calls are ignored). A worker that never calls `next` blocks its queue, and the whole `execQueue` call never completes: add your own timeout around slow or unreliable work.

- `err`: any truthy value marks the element as failed; it is retried on the next round
- `result`: any non-`null`/`undefined` value is collected into `results`
- `actions`: optional object:
  - `{ stop: true }`: stops **all** queues after their in-flight element; already collected results and errors are returned
  - `{ logs: [...] }`: entries appended to the queue logs, written to a JSON file at the end

A worker that throws synchronously does not crash the process: the exception is recorded as a normal element error.

```js
execQueue('sync-users', users, (user, next) => {
  api.sync(user, (err, res) => {
    if (err && err.code === 'QUOTA_EXCEEDED') {
      return next(null, null, { stop: true }); // stop everything
    }
    return next(err, res, { logs: [`synced ${user.id}`] });
  });
}, { concurrency: 5, delay: 100, retry: 2 }, (err, results, errors) => { /* ... */ });
```

### `setDefaultOptions(options)`

Define the options applied to every `execQueue` call, so a deployment-wide setting is not repeated at each call site. The options passed to `execQueue` always win, and each call **replaces** the previous defaults (call it with `null` or nothing to reset them).

```js
const { setDefaultOptions, execQueue } = require('@carboneio/cqueue');

setDefaultOptions({ concurrency: 10, logDir: '/var/log/myapp', logRetentionDays: 30 });

await execQueue('convert-files', files, worker);              // 10 queues, /var/log/myapp
await execQueue('sync-users', users, worker, { concurrency: 2 }); // 2 queues, same logDir
```

Accepts the same options as `execQueue`. The object is copied, so mutating it afterwards changes nothing, and the internal keys of the library are never read from it.

### `setLogFunction(fn)`

Replace the default logger (`console.log`) with your own `(message, level, fields) => {}`. Used by all internal logs (start/end summaries, error summaries, retries, file creation, progress without a TTY).

- `message`: the human readable line, unchanged
- `level`: `info`, `warn` or `error`
- `fields`: a new flat object of strings, numbers and booleans, always with `event` and `queue` (the queue name as a string). Never nested, never a reference to your elements: it can be serialized as is (logfmt, JSON) and indexed

A logger declared with two arguments `(message, level)` keeps working unchanged, and so does the default output, which never prints the fields. A logger that throws, or returns a rejected promise, is ignored: it never crashes the queue nor prevents its callback. A value that is not a function is ignored too: the current logger is kept.

The `fields` object is always passed as the 3rd argument, whatever the function declares: a variadic logger such as `console.log` prints it too. Wrap it to keep the message only: `setLogFunction((msg, level) => console.log(msg))`.

Every message is a single line (except the multi-line status block): control characters coming from the queue name or from error messages are replaced by a space, so a worker error cannot forge a log line. The `summary` and `error` fields follow the same rule.

**Forward the `level`**: everything that fails is logged at `error`, so a host logger mapping levels to syslog priorities makes queue failures visible to `journalctl -p err`. Dropping the argument reports every failure as `info`.

```js
const { setLogFunction } = require('@carboneio/cqueue');
setLogFunction((msg, level = 'info', fields) => myLogger.log(level, msg, fields));
```

| `event` | Level | When | Fields besides `event` and `queue` |
|---|---|---|---|
| `queue_start` | `info` | Start of each round, when `logEnabled` | `total` (elements of the round), `concurrency`, `delay`, `retry`, `try` |
| `queue_end` | `info` | Round ending without any error, when `logEnabled` | `total`, `results` (results returned to the caller, all rounds included), `errors` (`0`), `try`, `duration_ms` (wall clock of the round, files included), `avg_ms` (average time per element of the slowest queue, the `Avg time/exec` of the message) |
| `queue_errors` | `error` | Round ending with errors | `errors` (count), `try`, `summary` (`"ECONNREFUSED" x9, "ETIMEDOUT" x2, +1 more`) |
| `queue_retry` | `warn` | A retry round is about to start | `try` (number of the round about to start), `elements` (to retry), `delay_ms` (wait before the round) |
| `queue_failed` | `error` | Errors remain after the last round (`END - Stop retrying`) | `errors`, `try`, `total` (elements of the last round), `duration_ms` (of the last round) |
| `queue_crash` | `error` | Internal error, the callback receives `err` | `try`, `error` (message) |
| `queue_progress` | `info` | Status block without a TTY, when `logQueueStatus` | `try`, `queues`, `done`, `total`, `percent`, `eta_ms`, `errors`, summed over the queues of the round (`eta_ms` is the one of the slowest queue) |
| `log_file` | `error` for errors, `info` for logs | An errors / logs JSON file has been written | `label` (`errors` or `logs`), `path` |
| `log_file_error` | `error` | That file, or its folder, cannot be written | `label`, `path`, `error` (message) |
| `log_cleanup_error` | `warn` | `logRetentionDays` cannot list the folder or delete an old file | `label`, `path`, `error` (message) |

`try` is `0` for the first round, then `1`… up to `retry`. A run ends with exactly one `queue_failed` or `queue_crash` when it fails, or one `queue_end` when it succeeds (`queue_end`, like `queue_start`, only when `logEnabled`).

Without a registered function, the default output prefixes the level (`ERROR …`, `WARN …`) and leaves `info` messages unchanged.

#### Example: one logfmt line per message (Grafana Loki, `| logfmt`)

```js
const { setLogFunction } = require('@carboneio/cqueue');

/** Bare when safe, otherwise quoted with \ " and control characters escaped */
function logfmtValue (value) {
  const _str = String(value);
  if (_str !== '' && /^[^\s="\\\x00-\x1f]+$/.test(_str)) {
    return _str;
  }
  return '"' + _str.replace(/[\\"\x00-\x1f]/g, (c) => ({ '\\': '\\\\', '"': '\\"', '\n': '\\n', '\r': '\\r', '\t': '\\t' }[c] ?? ' ')) + '"';
}

setLogFunction((msg, level = 'info', fields = {}) => {
  let _line = `level=${level}`;
  for (const _key in fields) {
    _line += ` ${_key}=${logfmtValue(fields[_key])}`;
  }
  process.stdout.write(`${_line} msg=${logfmtValue(msg)}\n`);
});
```

```
level=info event=queue_start queue=upload total=2 concurrency=2 delay=0 retry=1 try=0 msg="[upload] START - 2 total elements - 2 queue(s) - 0ms delay - 0/1 retrie(s)"
level=error event=queue_errors queue=upload errors=1 try=0 summary="\"Error: bad line\" x1" msg="[upload] 1 errors: \"Error: bad line\" x1"
level=warn event=queue_retry queue=upload try=1 elements=1 delay_ms=1000 msg="[upload] Retry to re-execute the process on failled elements..."
level=error event=queue_failed queue=upload errors=1 try=1 total=1 duration_ms=4 msg="[upload] END - Stop retrying, check the error file!"
```

### `msToTime(ms)`

Format a duration: `999` → `"999 ms"`, `1500` → `"1.5 Sec"`, `90000` → `"1.5 Min"`, then `"Hrs"` and `"Days"`.

### `chunkify(list, size)`

Split a list into `size` balanced chunk descriptors (used internally, exported for testing/advanced use).

## Live status rendering

With `logQueueStatus: true`, the per-queue status block is rendered:

- **On a TTY**: repainted in place at most every 200ms (single atomic write, cursor hidden during the repaint, lines cleared and truncated to the terminal width). cqueue's own log messages are printed *above* the live block without corrupting it. Avoid `console.log` from your worker while a status block is active — printing through an external logger via `setLogFunction`, or after completion, keeps the display intact
- **Without a TTY** (CI, piped output): a plain status block is printed only when the round progresses by 10%, plus one final block, so logs are not flooded. It goes through the function registered with `setLogFunction` (at the `info` level), so the progress reaches the host's log file like every other message

## Retry delay

By default a retry round starts as soon as the previous one ends. Against a rate-limited or overloaded service, `retryDelay` spaces the rounds out with an exponential backoff:

```js
execQueue('upload', files, worker, { retry: 3, retryDelay: 1000 }); // waits 1s, 2s, then 4s before the rounds 1, 2 and 3
```

- The wait is capped to **60 seconds** per round, so a large `retry` cannot stall a scheduled job for hours, and the value always stays far below the `setTimeout` limit (about 24.8 days, above which Node.js fires the timer immediately)
- It is a regular timer: the event loop is never blocked, and the pending round keeps the process alive like any element still running, so the callback is always called. It is not `unref`'d on purpose: an unref'd timer would let a script whose only pending work is the retry exit silently, without results nor callback
- The wait is logged in the `delay_ms` field of the `queue_retry` event

## Error and log files

Each failed round logs the distinct error messages with their number of occurrences, most frequent first, capped to the top 3 followed by the count of the distinct messages left out, so the common failure is readable without opening any file. Each message is JSON-quoted (an embedded `"` is escaped) and truncated to 100 characters:

```
[convert-files] 12 errors: "Error: connect ECONNREFUSED" x9, "Error: ETIMEDOUT" x2, +1 more
```

When elements fail (see the `errorFile` option), or when workers provide `actions.logs`, a JSON file is written to the `logDir` directory (`<current working directory>/logs/` by default, created automatically). Files are serialized and written with a chunked streaming writer, so a huge errors/logs array never blocks the event loop:

```
2026-08-12T14-05-33-my-queue-errors.json       ← failures of the first attempt
2026-08-12T14-05-35-my-queue-errors-try1.json  ← failures of retry round 1
2026-08-12T14-05-35-my-queue-logs.json         ← collected actions.logs
```

With `errorFile: 'final'`, only the file of the last round is written (`…-errors-try1.json` above), and none when a retry recovers every element: one file per failed run, holding exactly the errors returned to the caller. With `errorFile: false`, no errors file at all, the `queue_errors` log line still carries the summary.

Files are created with the `wx` flag: the queue never follows a symlink nor overwrites an existing file in `logDir`. When the name is already taken, typically by another run of the same queue within the same second, the file is written once more under a unique name (`…-errors-<pid>-<n>.json`, still with `wx`, and covered by `logRetentionDays`); if that name is taken as well, the failure is logged at `error`.

The queue waits for these files to be fully written before calling back, so a caller exiting immediately — `execQueue(..., () => process.exit())` — always gets a complete file. The `Created … file` line is logged once the file is written. A file that cannot be written (or a write failing midway, such as a full disk, which stops the serialization) is logged at the `error` level, never discards the results or the errors returned to the caller, and the `END` line then does not ask to check the error file.

Use `logRetentionDays` to bound the directory:

```js
execQueue('convert-files', files, worker, { logDir: '/var/log/myapp', logRetentionDays: 30 });
```

## Exports

```js
const { execQueue, msToTime, chunkify, setLogFunction, setDefaultOptions, NS_PER_SEC, MS_PER_NS } = require('@carboneio/cqueue');
```

## Tests

```bash
npm test           # unit + performance tests, node:test runner, zero dependency
npm run test:unit  # unit tests only
npm run test:perf  # performance regression tests only
```

## License

Apache-2.0
