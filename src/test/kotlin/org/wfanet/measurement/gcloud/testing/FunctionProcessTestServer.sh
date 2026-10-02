#!/bin/sh

case "${FUNCTION_PROCESS_TEST_MODE:-ready}" in
  exit)
    exit 1
    ;;
  hang)
    ;;
  ignore_term)
    trap '' TERM
    echo "Serving function..."
    ;;
  ready)
    echo "Serving function..."
    ;;
  *)
    exit 2
    ;;
esac

exec sleep 2147483647
