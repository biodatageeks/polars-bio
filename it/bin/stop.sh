#!/usr/bin/env bash

docker-compose -f docker-compose.yml down
docker volume rm it_minio_data

docker stop azurite
docker rm azurite