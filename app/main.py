from pyspark.sql import SparkSession
from delta.tables import DeltaTable

if __name__ == '__main__':

    spark = (
        SparkSession
        .builder
        .appName('delta')
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel('ERROR')
    
    df = spark.range(1,10)
    df.show()
    
    df.coalesce(1).write.format('delta').save('./data/delta/bronze/example/')

    delta_table = DeltaTable.forPath(spark, './data/delta/bronze/example/')
    delta_table.history(1).show()

    spark.stop()