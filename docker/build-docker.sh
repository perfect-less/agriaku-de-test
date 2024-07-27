#! /bin/env /usr/bin/bash

echo "Building docker image with tag 'attendance-pipeline'"
docker build -t attendance-pipeline -f ./docker/Dockerfile .
