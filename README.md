# Tutorial
```sh
# install & configure asdf
brew install asdf

asdf plugin-add python

asdf list all python

asdf install python 3.9.13

# install pyspark & delta
!pip install delta-spark==1.2.1
!pyspark --version

# init Spark Session
spark = (
    SparkSession
    .builder
    .master('local[*]')
    .appName('Quickstart DeltaLake')
    .config('spark.jars.packages', 'io.delta:delta-core_2.12:1.2.1')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .getOrCreate()
)

# run spark app with delta
spark-submit \
    --packages io.delta:delta-core_2.12:1.2.1 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \ 
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```