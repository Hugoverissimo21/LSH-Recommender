#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
export SPARK_HOME=/users5/uvlabuaveiro/curso07/spark
export PATH=$SPARK_HOME/bin:$PATH

TMPDIR_BASE=/users5/uvlabuaveiro/curso07/tmp_local
mkdir -p "$TMPDIR_BASE"

echo "=== Iteração $i ==="

# cria diretório temporário único para esta execução
TMPDIR="$TMPDIR_BASE/$(date +%s%N)"
mkdir -p "$TMPDIR"

echo "Running on $file using tmpdir=$TMPDIR"

# executa spark-submit com variáveis forçadas para diretório temporário
SPARK_LOCAL_DIRS="$TMPDIR" TMPDIR="$TMPDIR" \
spark-submit \
    --master local[8] \
    --conf spark.local.dir="$TMPDIR" \
    --conf spark.driver.extraJavaOptions="-Djava.io.tmpdir=$TMPDIR" \
--conf spark.executorEnv.TMPDIR="$TMPDIR" \
    tuningHPC.py

# limpa diretório após execução
rm -rf "$TMPDIR"
sleep 5
