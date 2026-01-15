#!/bin/sh
# Ignore the first argument if it is "tritonserver"
if [ "$1" = "tritonserver" ]; then
  shift
fi

exec /opt/tritonserver/bin/tritonserver "$@"
