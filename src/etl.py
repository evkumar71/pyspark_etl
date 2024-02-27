from pyspark.sql import SparkSession
from prepare import Prepare
from datastore import DataStore
from appcontext import app_context


def main():
    app = app_context()
    obj_ds = DataStore(spark=app.spark, config=app.config['data'])
    obj_pre = Prepare(app.spark, obj_ds)
    obj_pre.process_sym()
    obj_pre.calc_avg()

    app.spark.stop()


if __name__ == '__main__':
    main()
