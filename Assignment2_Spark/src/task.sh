#!/bin/bash

pyspark \
--master yarn-client \
--num-executors 3 \
task1.py