# Changelog

All notable changes to this project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-08-26

### Added

- `setDefaultOptions(options)`: define the options applied to every `execQueue` call instead of repeating them at each call site. The options of the call win, a new call replaces the previous defaults, `null` resets them. The object is copied and the internal keys of the library are never read from it
- `logDir` option: directory of the generated error / log files, defaulting to the previous `<cwd>/logs` so nothing changes for existing users. An absolute path makes the files independent of the directory the process was started from
- `logRetentionDays` option: delete the error / log files of the queue older than X days after each successful write, fractions accepted. Defaults to `0`, which keeps every file. The age is read from the timestamp in the file name, so no `stat` call per file, and only the files matching that queue name and label are deleted — never the file of the current run

### Security

- Path traversal through the queue name: `execQueue('../../../pwned', ...)` wrote its JSON file outside the log directory, with content coming from the processed elements. Queue names are now reduced to `[a-z0-9._-]` for the file name, and a second guard refuses any path that does not resolve directly inside the log directory
- Symlink following in the log directory: the file name is the timestamp in seconds plus the queue name, so a local attacker able to write in that directory could pre-create a symlink and have the queue overwrite any file the process can write. Files are now opened with the `wx` flag, which fails instead of following a link or overwriting an existing file. A same-second name collision is now logged at `error` instead of silently overwriting the previous file

### Fixed

- A log or error entry that `JSON.stringify` cannot serialize (a circular structure, a `BigInt`) crashed the process from the chunked writer, and the queue callback was never called. Such an entry is now written as `null`, like the other non-serializable values

- The error / log files were written fire-and-forget: a caller exiting right after the queue, such as `execQueue(..., () => process.exit())`, could truncate or lose the file the error line asks the operator to check. The queue now waits for the write to complete before calling back, and a write failure is logged at `error` without discarding the results

### Changed

- Without a TTY, the per-queue status block now goes through the function registered with `setLogFunction` (at the `info` level) instead of writing to stdout directly, so the progress reaches the host's logger. The throttling (every 10% of progress plus a final block) and the TTY rendering are unchanged
- Log levels are now passed to the function registered with `setLogFunction`: failures are logged at `error` (error summary, error file path, `END - Stop retrying`, logs folder / file creation failures) and a retry round about to be attempted at `warn`. Previously every message reached the host logger as `info`
- The error line lists the distinct error messages with their number of occurrences, most frequent first, capped to the top 3 plus the count of the distinct messages left out: `[queue] 12 errors: "ECONNREFUSED" x9, "ETIMEDOUT" x2, +1 more`. The error JSON file is still written unchanged, and its path is now logged at `error` instead of `info`
- Emojis removed from the logged messages: the default output prefixes the level instead (`ERROR …`, `WARN …`). The live status block keeps its progress markers

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
