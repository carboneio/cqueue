# Changelog

All notable changes to this project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-08-12

First release as a dedicated repository.

### Added

- `execQueue(queueName, list, functionToExecute, options, callback)`: execute a function over a list of elements split across N parallel queues
- Options: `concurrency` (number of parallel queues), `delay` (ms between executions, rate limiting), `retry` (rounds re-executing only failed elements), `logEnabled`, `logQueueStatus`
- Callback mode `(err, results, errors)` and promise mode (omit the callback to `await` a `{ results, errors }` object)
- Remaining errors after the last retry round are returned as the third callback argument / `errors` property
- Worker actions: `{ stop: true }` stops all queues, `{ logs: [...] }` collects entries written to a JSON log file
- Live status rendering: on a TTY the per-queue progress block is repainted in place (throttled to 200ms, cursor hidden during repaint, lines cleared and truncated to the terminal width, log messages printed above the block); without a TTY a plain block is printed every 10% of progress plus a final one
- Error and log JSON files written to `<cwd>/logs/` with a chunked streaming writer (never blocks the event loop), one file per retry attempt
- Batched synchronous execution: elements completing synchronously run in a loop yielding back to the event loop every 8ms (~0.3µs per element) instead of one timer per element
- Robustness: synchronous worker throws are recorded as element errors, double calls of the worker callback are ignored, falsy results (`0`, `''`, `false`) are kept, the caller's options object is never mutated
- `setLogFunction(fn)` to replace the default logger, `msToTime(ms)` duration formatter, `chunkify(list, size)` helper, `NS_PER_SEC` / `MS_PER_NS` constants
- Test suite: 50 tests (42 unit + 8 performance regression) on the built-in `node:test` runner, zero dependency
- GitHub Actions CI on Node.js 22, 24 and 26, actions pinned by commit SHA
