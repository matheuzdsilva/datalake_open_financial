#!/bin/bash
set -e

echo 
cd src
zip -r /tmp/src.zip . > /dev/null
cd ..

echo "Executando Spark job"
docker cp /tmp/src.zip spark:/src.zip
docker exec -it spark spark-submit \
  --master local[*] \
  --py-files /src.zip \
  /src/processing/bronze_to_silver.py
