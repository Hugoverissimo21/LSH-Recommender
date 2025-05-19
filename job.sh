#!/bin/bash
for i in {1..2}; do
    echo "=== Iteração $i ==="
    for file in data/10M.csv data/20M.csv data/25M.csv; do
        echo "Running on $file"
        spark-submit --master local[8] deploy.py "$file"
    done
done
