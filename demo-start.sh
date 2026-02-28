#!/usr/bin/env bash
# Convenience stub — delegates to the root demo-start.sh
# Run from anywhere: ./demo-start.sh, hello_world/demo-start.sh, etc.
exec "$(dirname "${BASH_SOURCE[0]}")/../demo-start.sh" "$@"
