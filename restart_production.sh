#!/bin/sh

sudo docker-compose -f docker-compose-prod.yml down
sudo docker container ls -a | awk '{print $1}' | xargs sudo docker container rm
sudo docker image ls | awk '{print $3}' | xargs sudo docker image rm
sudo docker volume ls | awk '{print $2}' | xargs sudo docker volume rm
sudo docker-compose -f docker-compose-prod.yml up -d
