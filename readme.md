
[![Go Report Card](https://goreportcard.com/badge/github.com/marvinusarif/redis-pipeline)](https://goreportcard.com/report/github.com/marvinusarif/redis-pipeline)

# redis-pipeline
---
this library is intended to replace redis worker pool by batching commands from multiple requests. All commands will be sent into single redis pipeline connection for single node redis where in the redis cluster environment, the batched commands will be pipelined into relevant nodes respectively.
Every batch will be executed in 10 Millisecond interval or whenever the maxCommandsPerBatch is reached.

Examples are provided.

This library uses :
1. [redis-go-cluster](https://github.com/chasex/redis-go-cluster)
2. [redi-go](https://github.com/gomodule/redigo)

Ccheck folder is command shell script to spawn redis cluster in your local machine. Please see [README](https://github.com/marvinusarif/ccheck/README.md).
