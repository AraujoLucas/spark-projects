from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext

glueContext = GlueContext(SparkContext.getOrCreate())

# Configurando o tamanho máximo de cada split em bytes
glueContext.getConf().set("mapred.max.split.size", "5368709120") # 5 GB

# Criando um DynamicFrame a partir de uma fonte de dados
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="mydatabase",
    table_name="mytable"
)

# Processando o DynamicFrame como necessário
# ...

# Salvando o DynamicFrame em uma tabela de destino
glueContext.write_dynamic_frame.from_options(
    frame=datasource,
    connection_type="s3",
    connection_options={
        "path": "s3://mybucket/output"
    },
    format="parquet"
)
