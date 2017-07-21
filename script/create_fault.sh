#!/usr/bin/env bash

set -x
curl -X PUT -d '{"start_time_ms":1500666845443, "end_time_ms":1500666890443, "spec":{"type":"noop"}}' localhost:8888/faults
