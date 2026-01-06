#!/bin/bash
# Helper script to run Redis tests with proper Java 11 and Docker configuration


# Set Java 11 if not already set
if [ -z "$JAVA_HOME" ]; then
  # Try to auto-detect Java 11
  if command -v java >/dev/null 2>&1; then
    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$JAVA_VERSION" != "11" ]; then
      echo "Warning: Java 11 required but version $JAVA_VERSION found"
    fi
  else
    echo "Error: Java not found. Please set JAVA_HOME or install Java 11"
    exit 1
  fi
fi

# Set Docker socket if not already set (try common locations)
if [ -z "$DOCKER_HOST" ]; then
  if [ -S "$HOME/.rd/docker.sock" ]; then
    export DOCKER_HOST=unix://$HOME/.rd/docker.sock
  elif [ -S "/var/run/docker.sock" ]; then
    export DOCKER_HOST=unix:///var/run/docker.sock
  fi
fi

# Disable Testcontainers Ryuk for faster test execution
export TESTCONTAINERS_RYUK_DISABLED=true

# Run the tests
cd "$(dirname "$0")/.." || exit

# If no arguments, run all tests; otherwise pass through arguments
if [ $# -eq 0 ]; then
  ./mill redis.test
else
  ./mill redis.test "$@"
fi

