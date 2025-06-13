#!/bin/bash

echo "WARNING: This will stop all containers, remove volumes, orphans, rebuild everything, and start fresh."
read -p "Are you sure you want to continue? (y/N): " confirm

if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
    docker-compose down -v --remove-orphans
    docker-compose build --no-cache
    docker-compose up
else
    echo "Aborted."
fi