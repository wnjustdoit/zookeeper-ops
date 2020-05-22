#!/usr/bin/env bash
echo 'building is starting...'
cd zk-lock/ && mvn clean install deploy
