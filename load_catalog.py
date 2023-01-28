from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext

def create_dynamic_frame(database, table_name):
    glueContext = GlueContext(SparkContext.getOrCreate())
    glueContext.getConf().set("mapred.max.split.size", "5368709120") # 5 GB
    return glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table_name
    )

def write_dynamic_frame(frame, path):
    glueContext = GlueContext(SparkContext.getOrCreate())
    glueContext.write_dynamic_frame.from_options(
        frame=frame,
        connection_type="s3",
        connection_options={
            "path": path
        },
        format="parquet"
    )

# Usage
datasource = create_dynamic_frame("mydatabase", "mytable")

# Processando o DynamicFrame como necessário
# ...

write_dynamic_frame(datasource, "s3://mybucket/output")
