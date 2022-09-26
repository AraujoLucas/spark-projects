# spark-projects
Repo for storage my scritps pyspark

# Glue Job load in data mesh example

Configuration:

Type: Spark

Glue version: Glue 3.0 - Supports spark 3.1, Scala 2, Python 3

Language: Python 3

Worker Type: G 1x

Spark UI logs path: s3://key/logs/
  
Temporary path: s3://key/tmp/
  
Job parameters: 
  
    Key: --glue_params
  
    Value: [{"db_target":"db","tb_target":"tb","account-id":"xxx", "data_source_1":"s3://key/fonte_1/json/", "data_source_2":"s3://key/fonte_2/csv/"}]
