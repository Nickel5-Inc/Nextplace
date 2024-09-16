#!/bin/bash

node=$1

rm /root/.pm2/logs/"${node}"-error.log
rm /root/.pm2/logs/"${node}"-out.log