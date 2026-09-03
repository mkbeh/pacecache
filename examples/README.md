# Examples

This directory contains runnable examples demonstrating the main `pacecache` usage patterns.

| Example            | Demonstrates                                                                                                   |
|--------------------|----------------------------------------------------------------------------------------------------------------|
| [`basic`](./basic) | Cache-aside loading, direct lookups, entry metadata, not-found loader behavior, deletion, and cache statistics |
| [`otel`](./otel)   | Exporting `pacecache` metrics with the optional `extra/paceotel` OpenTelemetry integration                     |

## Running the examples

Run an example from its directory:

```bash
cd basic
go run .
```

Or run it from the repository root:

```bash
go run ./examples/basic
```

The examples are self-contained and do not require external services. Refer to the README in each example directory for
the demonstrated workflow and additional details.
