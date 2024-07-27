#! /bin/env /usr/bin/bash

echo "Running docker image with tag 'attendance-pipeline'"

echo "creating docker_volume directory"
mkdir $(pwd)/docker_volume
echo "creating docker_volume/warehouse directory"
mkdir $(pwd)/docker_volume/warehouse
echo "creating docker_volume/exported-data directory"
mkdir $(pwd)/docker_volume/exported-data
echo "copying 'raw-data/' to 'docker_volume'"
cp -r raw-data/ docker_volume/

docker run --rm \
    -v $(pwd)/docker_volume/warehouse/:/att-pipe/warehouse \
    -v $(pwd)/docker_volume/raw-data/:/att-pipe/raw-data \
    -v $(pwd)/docker_volume/exported-data/:/att-pipe/exported-data attendance-pipeline 
