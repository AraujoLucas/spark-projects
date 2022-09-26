import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
from datetime import datetime

client = boto3.client('glue')

# function extract params for initializing job etl for load in catalog
def load_files(glueContext,spark, glue_params: dict):
    # extrac params
    db_target = glue_params['db_target']
    tb_target = glue_params['tb_target']
    account = glue_params['account-id']
    data_source_1 = glue_params['data_source_1']
    data_source_2 = glue_params['data_source_2']
    data_target = glue_params['data_target']
    now = datetime.now()
    date_process = "%s%02d%s" % (now.year, now.month,now.day)
    
    print(f'{datetime.now()} --- Account {account}')
    print(f'{datetime.now()} --- Database target for load {db_target}')
    print(f'{datetime.now()} --- Table for ingeston {tb_target}')
    print(f'{datetime.now()} --- Data_source_1 input for load {data_source_1}')
    print(f'{datetime.now()} --- Data_source_2 input for load {data_source_2}')
    print(f'{datetime.now()} --- Data_target for load results {data_target}')
    
    print(f'{datetime.now()} --- Loading files in path {data_source_2}')
    df_sales = read_csv(data_source_2, spark)
    print(f'{datetime.now()} --- Loading files in path {data_source_1}')
    df_users = read_json(data_source_1,spark)
    return df_sales, df_users,db_target,tb_target,path_output_cc,path_output_cd

# function for read files in format csv 
def read_csv(data_source,spark):
    print(f'{datetime.now()} --- Initializing load in memory')
    df_csv = spark.read.load(
        path=data_source,
        format='csv',
        sep=',',
        inferSchema='true',
        header='true')
    print(f'{datetime.now()} --- DF load in memory')
    df_csv.show(3)
    return df_csv
    
# function for read files in format json
def read_json(data_source,spark):
    print(f'{datetime.now()} - Initializing load in memory')
    df_json = spark.read.load(
        path=data_source,
        format='json')
    print(f'{datetime.now()} - DF load in memory')
    df_json.show(3)
    return df_json

# function apply business rules
def apply_rule(df_csv,df_json):
    print(f'{datetime.now()} --- Initialing join dfs in memory')
    df_join = df_csv.join(
        df_json, df_csv.id_vendedor == df_json.id)
    print(f'{datetime.now()} --- DF results the join')
    df_join.show(3)
    
    print(f'{datetime.now()} --- Apply transformation')
    df_results = df_join.select(
        df_json.nome,
        df_json.cidade,
        df_json.status, 
        df_csv.id_produto, 
        df_csv.forma_pagamento).filter(
            (df_csv.forma_pagamento == 'cc') & (df_json.status == 'Ativo'))
    print(f'{datetime.now()} --- End job')
    return df_results

# function for convert df in dfy
def convert_df_to_dy(df,glueContext):
    dfy_load = DynamicFrame.fromDF(
        df, glueContext, "nested")
    print(f'{datetime.now()} --- DynamicFrame for load in catalog')    
    dfy_load.printSchema()
    return dfy_load

# function save table final in metastore
def save_table(dfy,glueContext,db_target,tb_target):
    glueContext.write_dynamic_frame.from_catalog(
        frame=dfy, 
        database=db_target, 
        table_name=tb_target)
    return print(f'{datetime.now()} --- Load in table metastore sucess!')
    
# function main the program
def main():
    print(f'{datetime.now()} --- Solving arguments \n')
    glue_params = getResolvedOptions(sys.argv, ['JOB_NAME','glue_params'])
    print(f'{datetime.now()} --- Solved {glue_params} params\n')
    
    sc = SparkContext()
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    job = Job(glueContext)

    job.init(glue_params['JOB_NAME'], glue_params)
    
    for each_param in json.loads(glue_params["glue_params"]):
        print(f'{datetime.now()} --- initializing glue job with job params {each_param}.\n')
        try:
            print(f'{datetime.now()} --- Initialing load files in memory')
            df_csv,df_json,db_target,tb_target = load_files(glueContext,spark, each_param)
       
            print(f'{datetime.now()} --- Filter in df join')
            df_sales_cc = apply_rule(df_csv,df_json)
                
            print(f'{datetime.now()} --- Convert df to dfy')
            dfy_load = convert_df_to_dy(df_sales_cc,glueContext)

            print(f'{datetime.now()} --- Save table in metastore')
            save_table(dfy_load,glueContext,db_target,tb_target)

        except Exception as e:
            print(f'Error {e} in processing data verify logs')

    job.commit()
    
if __name__ == '__main__':
    main()
