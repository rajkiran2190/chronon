#!/bin/bash
# Helper script to run Redis tests with proper Java 11 and Docker configuration

# Set Java 11 (required for compatibility)
export JAVA_HOME=/Library/Java/JavaVirtualMachines/openjdk-11.jdk/Contents/Home

# Set Docker socket for Rancher Desktop (adjust if using different Docker setup)
export DOCKER_HOST=unix://$HOME/.rd/docker.sock

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

