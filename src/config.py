app_config = {
    "spark": {
        "app": "etl-loader",
        "master": "local[*]"
    },
    'data': {
        "base_dir": "data/",
        "raw_layer": "data/raw/",
        "drv_layer": "data/derived/"
    }
}