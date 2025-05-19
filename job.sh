#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.422.b05-2.el8.x86_64/jre
export SPARK_HOME=/users5/uvlabuaveiro/curso07/spark
export PATH=$SPARK_HOME/bin:$PATH
export SPARK_LOCAL_DIRS=/tmp/spark

for i in {1..5}; do
    echo "=== Iteração $i ==="
    for file in data/100k.csv data/1M.csv data/10M.csv data/20M.csv data/25M.csv; do
        echo "Running on $file"
        spark-submit --master local[8] deploy.py "$file" -bucketLength 1.5 -numHashTables 2 -threshold 1.2

        sleep 5
        rm -rf /users5/uvlabuaveiro/curso07/spark_tmp/*
        sleep 5
    done
done