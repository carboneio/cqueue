# cqueue

Zero-dependency concurrent queue executor for Node.js. Split a list of elements into N parallel queues, execute an asynchronous function on each element, and get aggregated results, errors, automatic retries, live progress and performance stats.

## Features

- 🚀 **Concurrency**: split the work across N parallel queues
- 🔁 **Automatic retries**: failed elements are re-executed, remaining errors are returned
- ⏱ **Delay**: optional pause between each execution (rate limiting)
- 🛑 **Stop action**: any element can stop all queues
- 📊 **Live progress**: per-queue percentage, average time per execution, estimated time left
- 📄 **Error / log files**: failures and custom logs are written as JSON files
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
| `queueName` | `String` | Name used in logs and generated file names |
| `list` | `Array` | Elements to process (objects, strings, numbers…) |
| `functionToExecute` | `Function` | Worker `(element, next) => {}` executed for each element |
| `options` | `Object` | Optional, see below |
| `callback` | `Function` | Optional `(err, results, errors) => {}`. When omitted, a promise resolving `{ results, errors }` is returned |

#### Options

| Option | Type | Default | Description |
|---|---|---|---|
| `concurrency` | `Number` | `1` | Number of parallel queues. The list is split into `concurrency` balanced chunks, each processed sequentially |
| `delay` | `Number` | `0` | Milliseconds to wait between each execution in a queue (rate limiting). With `0`, elements completing synchronously are executed in batches that yield back to the event loop every few milliseconds: no artificial delay, no event loop starvation |
| `retry` | `Number` | `1` | Number of extra rounds re-executing only the failed elements. `0` disables retries |
| `logEnabled` | `Boolean` | `true` | Log the START line and the END performance summary. Errors are still logged when `false` |
| `logQueueStatus` | `Boolean` | `true` | Live per-queue progress on stdout (`[0] 45% - 45/100 - Passed time: 2.1 Sec \| Left Time: 2.5 Sec \| Avg time/exec: 47 ms`). See [Live status rendering](#live-status-rendering) |

#### Callback / promise result

- `err`: internal execution error (`null` in normal operation, even when elements fail — element failures are reported through `errors`)
- `results`: every non-`null`/`undefined` value passed as `next(null, result)`, aggregated across all queues and retry rounds. Falsy values such as `0`, `''` and `false` are kept
- `errors`: elements still failing after the last retry round, as `[{ element, message }]`

### The worker function: `(element, next)`

Call `next(err, result, actions)` exactly once per element (extra calls are ignored):

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

### `setLogFunction(fn)`

Replace the default logger (`console.log`) with your own `(message, level) => {}`. Used by all internal logs (start/end summaries, error counts, file creation).

```js
const { setLogFunction } = require('@carboneio/cqueue');
setLogFunction((msg) => myLogger.info(msg));
```

### `msToTime(ms)`

Format a duration: `999` → `"999 ms"`, `1500` → `"1.5 Sec"`, `90000` → `"1.5 Min"`, then `"Hrs"` and `"Days"`.

### `chunkify(list, size)`

Split a list into `size` balanced chunk descriptors (used internally, exported for testing/advanced use).

## Live status rendering

With `logQueueStatus: true`, the per-queue status block is rendered:

- **On a TTY**: repainted in place at most every 200ms (single atomic write, cursor hidden during the repaint, lines cleared and truncated to the terminal width). cqueue's own log messages are printed *above* the live block without corrupting it. Avoid `console.log` from your worker while a status block is active — printing through an external logger via `setLogFunction`, or after completion, keeps the display intact
- **Without a TTY** (CI, piped output): a plain status block is printed only when a queue progresses by 10%, plus one final block, so logs are not flooded

## Error and log files

When elements fail, or when workers provide `actions.logs`, a JSON file is written to `<current working directory>/logs/` (created automatically). Files are serialized and written with a chunked streaming writer, so a huge errors/logs array never blocks the event loop:

```
2026-08-12T14-05-33-my-queue-errors.json       ← failures of the first attempt
2026-08-12T14-05-35-my-queue-errors-try1.json  ← failures of retry round 1
2026-08-12T14-05-35-my-queue-logs.json         ← collected actions.logs
```

## Exports

```js
const { execQueue, msToTime, chunkify, setLogFunction, NS_PER_SEC, MS_PER_NS } = require('@carboneio/cqueue');
```

## Tests

```bash
npm test           # unit + performance tests, node:test runner, zero dependency
npm run test:unit  # unit tests only
npm run test:perf  # performance regression tests only
```

## License

Apache-2.0
