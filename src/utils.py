from pyspark.sql import DataFrame


def rename_columns(df: DataFrame):
    dic = {}
    for col in df.columns:
        lis = col.split()
        new_lis = list(map(lambda w: w.capitalize(), lis))
        new_lis[0] = new_lis[0].lower()
        dic[col] = ''.join(new_lis)

    return df.withColumnsRenamed(dic)
