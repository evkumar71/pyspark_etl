app_config = {
    "spark": {
        "spark.app.name": "etl-loader",
        "spark.master": "local[*]"
    },
    'data': {
        "base_dir": "data/",
        "raw_layer": "data/raw/",
        "drv_layer": "data/derived/"
    }
}