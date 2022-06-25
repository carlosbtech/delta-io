
```sh
spark-submit --packages io.delta:delta-core_2.12:1.2.1 \
 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \ 
 --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```