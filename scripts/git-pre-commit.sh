#!/bin/bash

# Git pre-commit hook to run lint and tests before committing.
# If any of these commands fail, the commit will be aborted.

echo "Running git pre-commit hook: make lint..."
make lint
if [ $? -ne 0 ]; then
    echo "Error: make lint failed. Commit aborted."
    exit 1
fi

echo "Running git pre-commit hook: make test..."
make test
if [ $? -ne 0 ]; then
    echo "Error: make test failed. Commit aborted."
    exit 1
fi

echo "Pre-commit checks passed successfully."
exit 0
