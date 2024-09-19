#!/bin/bash

node=$1
num_lines=$2

tail /root/.pm2/logs/"${node}"-out.log -n ${num_lines}