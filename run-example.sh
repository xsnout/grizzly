#!/bin/bash

EXAMPLE=synthetic-slice-time-live
EXAMPLE_DIR=/tmp/jobs/${EXAMPLE}

rm -rf ${EXAMPLE_DIR}
mkdir -p /tmp/jobs
cp -r ~/git/xsnout/ursa/examples/${EXAMPLE} /tmp/jobs
JOB_DIR=${EXAMPLE_DIR} make build
