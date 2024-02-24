from pyspark.sql import SparkSession
from prepare import Prepare
from datastore import DataStore
from config import app_config

if __name__ == '__main__':
    spark_ses = SparkSession \
        .builder \
        .config(map=app_config['spark']) \
        .getOrCreate()

    obj_ds = DataStore(spark=spark_ses, config=app_config['data'])
    obj_pre = Prepare(spark_ses, obj_ds)
    obj_pre.process_sym()
    obj_pre.calc_avg()

    spark_ses.stop()
