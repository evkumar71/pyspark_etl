app_config = {
    "spark": {
        "app": "etl-loader",
        "master": "local[*]"
    },
    "data": {
        "base_dir": "data/",
        "raw_layer": "base_dir/raw/",
        "drv_layer": "base_dir/derived/"
    }
}