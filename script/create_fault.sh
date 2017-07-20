#!/usr/bin/env bash

set -x
curl -X PUT -d '{"start_time_ms":0, "end_time_ms":99999, "spec":{"type":"kill_node"}}' localhost:8888/faults
