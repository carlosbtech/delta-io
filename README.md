# Tutorial
```sh
# install & configure asdf
brew install asdf

asdf plugin-add python

asdf list all python

asdf install python 3.10.0

# create env python
python -m venv venv

source venv/bin/activate

# install pyspark & delta
pip install delta-spark==1.2.1

# run spark app with delta
spark-submit \
    --packages io.delta:delta-core_2.12:1.2.1 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \ 
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```