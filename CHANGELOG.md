# Changelog

All notable changes to this project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-11-19

### Added

- `setDefaultOptions(options)`: set options once for every `execQueue` call. Options passed to a call still win.
- `logDir`: where error / log files are written (default `<cwd>/logs`, as before).
- `logRetentionDays`: delete the queue's own files older than X days (default `0`, keep everything).
- `errorFile`: `true` (default, as before) writes an errors file after each failed round, `'final'` only once after the last round (and none if a retry fixes everything), `false` never.
- `retryDelay`: wait before each retry round, doubling each time, capped at 60 s (default `0`, retry immediately as before).
- Structured logs: the `setLogFunction` callback receives a 3rd argument `fields` (`event`, `queue`, `duration_ms`, `errors`…), ready for logfmt/JSON loggers. Two-argument loggers keep working. The events are listed in the README.

### Changed

- Log levels are passed to `setLogFunction`: failures at `error`, retries at `warn` (everything was `info`).
- A variadic logger such as `setLogFunction(console.log)` now also prints the fields object; wrap it: `(m, l) => console.log(m)`.
- The error line lists the most frequent messages, JSON-quoted: `[queue] 12 errors: "ECONNREFUSED" x9, "ETIMEDOUT" x2, +1 more`.
- The error file path is logged at `error` (was `info`), and only once the file is written.
- Without a TTY, progress goes through `setLogFunction` instead of stdout.
- No more emojis in messages; the default output prefixes the level (`ERROR …`, `WARN …`).
- The internal error passed to the callback lost its emoji too (`[queue] Error: Promise All Catched: …`): update any code matching the old `🚩` string.
- A `setLogFunction` callback that throws, or returns a rejected promise, no longer crashes the queue.

### Fixed

- The queue now waits for the error / log file to be written before calling back: `execQueue(..., () => process.exit())` no longer loses the file.
- An element that `JSON.stringify` cannot serialize (circular, `BigInt`) no longer crashes the process; it is written as `null`.
- Two runs of the same queue in the same second no longer lose a file: the second one gets a unique `-<pid>-<n>` suffix.
- A callback that throws on a failed run is no longer called a second time with an internal error: the throw propagates, as on success. In callback mode it becomes an unhandled rejection.
- A number as queue name no longer crashes (`queueName.replace is not a function`).
- No more `check the error file!` when that file could not be written; a write failing midway (disk full) stops serializing.
- An error that cannot become a string (`Object.create(null)`, a throwing `toString`) or a throwing `logs` getter no longer hangs the queue or crashes the process; it is recorded as `Unknown error`.
- A non-numeric `retry` (`NaN`, `'abc'`, `{}`) or `retry: Infinity` no longer retries forever; invalid `concurrency`, `delay`, `retryDelay` and `logRetentionDays` fall back to their defaults too.
- Retry rounds yield to the event loop, so a worker failing synchronously no longer freezes timers and I/O.
- An invalid `concurrency` no longer drops every element or crashes, and a huge one no longer runs out of memory: there are never more queues than elements.
- Many queues no longer slow down the end of the run nor the status without a TTY, which is now printed every 10% of the whole round.
- A `list` that is not iterable, an object as `logDir`, or a queue name that cannot become a string is now returned to the callback instead of an unhandled rejection.
- `setLogFunction` with something else than a function is ignored, instead of silently dropping every log.
- A very long queue name no longer fails the file write with `ENAMETOOLONG`: it is truncated to 200 characters in the file name.

### Security

- A queue name such as `../../x` wrote its file outside the log directory: names are now sanitized and the path is checked.
- A newline in an error message or a queue name could forge a fake log line: control characters are now replaced by a space.
- Files are created with the `wx` flag, so a symlink planted in the log directory can no longer redirect the write.
- Error and log files hold raw elements, so they are now created with mode `0600`, and a new log directory with `0700`.
- Bidi control characters (U+200E, U+200F, U+202A–U+202E, U+2066–U+2069) are replaced too, so a name can no longer reorder a log line.
- The internal options can no longer be forged by the caller (`_cqueue: true` skipped the validation).
- The publish workflow can no longer be started by hand, and no workflow keeps the git credentials after checkout.

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
