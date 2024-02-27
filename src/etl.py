from pyspark.sql import SparkSession
from prepare import Prepare
from datastore import DataStore
from appcontext import app_context


def main():
    context = app_context()
    obj_ds = DataStore(context)
    obj_pre = Prepare(context.spark, obj_ds)
    obj_pre.process_sym()
    obj_pre.calc_avg()

    context.spark.stop()


if __name__ == '__main__':
    main()
