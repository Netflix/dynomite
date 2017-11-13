#!/bin/bash
# check a single stats port
curl -s 127.0.1.2:22222/info | python -mjson.tool > /dev/null
