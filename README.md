The basic project structure is as shown below:

root/
 |-- configs/
 |   |-- etl_config.json
 |-- dependencies/
 |   |-- logging.py
 |   |-- spark.py
 |-- jobs/
 |   |-- etl_job.py
 |-- tests/
 |   |-- test_data/
 |   |-- | -- stocks/
 |   |-- | -- trading/
 |   |-- test_etl_job.py
 |   build_dependencies.sh
 |   packages.zip
 |   Pipfile
 |   Pipfile.lock
