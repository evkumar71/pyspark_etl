from appcontext import AppContext
from datastore import DataStore
import sys

class Prepare:

    def __init__(self, context):
        self.obj_ds = DataStore(context)

    def process_sym(self, symbol):
        df_meta = self.obj_ds.load_metadata()
        df_meta.show(5)

        obj_ds = self.obj_ds
        df_csv = obj_ds.load_symbol(symbol)
        obj_ds.write_target(df_csv, symbol)
        df_parq = obj_ds.read_target(symbol)
        df_parq.show(2)


def main():
    if len(sys.argv) > 1:
        context = AppContext(sys.argv[1])
    else:
        context = AppContext("config/config.json")
    obj_pre = Prepare(context)
    obj_pre.process_sym('ZUO')

    context.spark.stop()


if __name__ == '__main__':
    main()
