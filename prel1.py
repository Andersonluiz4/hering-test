# import td_pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, TimestampType
from pyspark.sql.functions import col,when,coalesce,lit,concat,from_json,when,row_number, sha2
from pyspark.sql.window import Window
import s3fs
import yaml
from datetime import datetime

def shape(df):
    print(df.count(), len(df.columns))

def new_base_service(table_name, table_schema, log=False):

    # create spark obj
    spark = SparkSession \
        .builder \
        .appName("session") \
        .getOrCreate()


    # disable spark logs
    spark.sparkContext.setLogLevel("ERROR")


    # create aws connection
    fs = s3fs.S3FileSystem(
        key='',
        secret=""
    )

    # if log flag is enabled
    # then all log process of prints and records
    # will be executed
    if log:
        # get start of the exec
        ini = datetime.now()

        # function for time execution calculation
        def pDif(ini, step='', record=False):

            # get actual time
            now = datetime.now()

            # get delta of initial time run
            exec_time_seconds = int((now - ini).total_seconds())

            # create a step for print and logs
            step = f"## {step} {exec_time_seconds} seconds"
            print(step)

            # add new row in 
            now_str = now.strftime('%Y-%m-%d %H:%M:%S')
            global log_content
            log_content = log_content + f"{step} | {now_str}\n"

            if record:
                with fs.open(f'cdp-becks/plogs/prel1/{now_str}.txt', 'w') as f:
                    f.write(log_content)
                    print('## Writed log')

        # log process
        global log_content
        log_content = ''

    # read columns master config file
    with fs.open('cdp-becks/scripts/config/columnsMaster.yml') as yfile:
        columns_master = yaml.safe_load(yfile) 
    if log: pDif(ini, '0. Loaded config file')

    # tools
    def run_query(qry):
        # create a view available from sql select
        spark.catalog.dropTempView('dataset')
        df.createOrReplaceTempView('dataset')
        return spark.sql(qry)

    # create schema for column name dataset
    schema = StructType() \
        .add("table_name",StringType(), False) \
        .add("column_name",StringType(),False) \
        .add("column_dtype",StringType(),False) \
        .add("column_order",IntegerType(),False) \
        .add("time",StringType(),False)

    # read columns_name dataset
    dataset_columns_tsv = spark.read \
        .schema(schema) \
        .option("delimiter", "\\t") \
        .csv(f's3://cdp-becks/{table_schema}/{table_name}__schema/*')
    if log: pDif(ini, '1. Loaded table schema of dataset from s3')
    dataset_columns_tsv.show()

    # drop schema
    schema = None

    # sort by column order
    dataset_columns_tsv = dataset_columns_tsv.orderBy('column_order')

    # create a function alias for schema creation
    # dtype_translate = {
    #     'varchar': StringType(),
    #     'bigint': IntegerType(),
    #     'array(varchar)': StringType(), # ArrayType(StringType()),
    #     'timestamp': TimestampType()
    # }

    # # create schema from column_name dataset
    # schema = StructType()
    # for row in dataset_columns_tsv.collect():
        
    #     # columns has basic dtype, use dtype from yaml
    #     if row['column_name'] in columns_master['basic_dtype']: 
    #         dtype = dtype_translate[columns_master['basic_dtype'][row['column_name']]]
    #     elif row['column_dtype'] not in dtype_translate.keys():
    #         dtype = dtype_translate['varchar']
    #     else:
    #         dtype = dtype_translate[row['column_dtype']]
            
    #     # add column in spark schema
    #     schema.add(row['column_name'], dtype, True)
    # if log: pDif(ini, "1.1 Structure builded")
        
    # read table data
    dataset_tsv = spark.read \
        .option("delimiter", "\\t") \
        .csv(f's3://cdp-becks/{table_schema}/{table_name}/*')
    if log: pDif(ini, "1.2 Table loaded from s3")

    dataset_tsv = dataset_tsv.select(
        *[col(f'_c{i}').alias(c.column_name) 
            for i, c in enumerate(dataset_columns_tsv.collect())])

    # replace null as string (\N) for NULL
    dataset_tsv = dataset_tsv.select([when(col(c)=="\\N",None) \
        .otherwise(col(c)) \
        .alias(c) for c in dataset_tsv.columns])

    dataset_tsv = dataset_tsv.withColumn("global_unique_id",
        # select all column and new one with all values concated and hashed
        sha2(concat(*[
            coalesce(col(c).cast('string'), lit('')) 
            for c in dataset_tsv.columns], lit(table_name)), 256))
    if log: pDif(ini, "1.3 Created global unique by row global_unique_id")

    # create a window for r_id creation
    w = Window().orderBy(lit('A'))
    #df = df.withColumn("row_num", row_number().over(w))

    # create new column r_id for global_unique_id creation
    dataset_tsv = dataset_tsv.withColumn("r_id", row_number().over(w))

    # alias dataset
    df = dataset_tsv
    df.show(10)

    # iter over each mapping columns and group same columns in one unique column
    # using key of dict as column alias
    for k, col_map in columns_master['columns_map'].items():
        
        # concat key in columns list name
        list_of_columns = [k] + col_map
        
        # check if table has columns
        cleaned_list_of_columns = [df[col] for col in list_of_columns if col in df.columns]
        
        # if has columns to map and transform
        if cleaned_list_of_columns:
            
            # build select with coalesce
            select_list = [col for col in df.columns if col != k]
            df = df.select(*select_list, coalesce(*cleaned_list_of_columns).alias(k))
    if log: pDif(ini, "2.1 Columns mapped")

    # go over each columns_map parameter
    for k, col_concat in columns_master['columns_map_concat'].items():
        
        # if dataset has all columns used in the concat, then concat them 
        if len([col for col in df.columns if col in col_concat]) == len(col_concat):
            
            # create a list of columns to concat splited by '-'
            list_of_columns_to_concat = []
            for i, c in enumerate(col_concat):
                list_of_columns_to_concat.append(col(c))
                
                # create a separator from concat
                if i != len(col_concat)-1:
                    list_of_columns_to_concat.append(lit('-'))
            
            # create new column with concated values as alias key+'_concat'
            df = df.withColumn(k + '_concat', concat(*list_of_columns_to_concat))
            
            # coalesce with new columns with that the already exist
            # ex: abi_dateofbirth with abi_dateofbirth_concat        
            df = df.select(
                # columns that are not inside coalese
                *[col for col in df.columns if col not in [k, k+'_concat']], 
                # coalesce between new column and old column
                coalesce(k+'_concat', k).alias(k))
    if log: pDif(ini, "2.2 Columns concated")

    # get columns normalization that dataset has
    columns_to_normalize = {
        k: v for k, v in columns_master['normalization'].items()
        if k in df.columns
    }
    # build a string as a select for each element
    list_of_normalization_select = { 
        col_name: f"{col_logic.replace('__key__',col_name)} as {col_name}"
        for col_name, col_logic in columns_to_normalize.items()
        if ('json' not in col_logic.lower()) and ('try' not in col_logic.lower())}

    # cleand differences between pyspark and presto
    cleaned_normalization_select = [
        col.replace("''","'").replace('VARCHAR','STRING')
        for col in list_of_normalization_select.values()
    ]

    # create a view available from sql select
    spark.catalog.dropTempView('dataset')
    df.createOrReplaceTempView('dataset')

    # run all normalization via sql function
    df = spark.sql(f"""
        select 
        {','.join(cleaned_normalization_select)},
        {','.join([col for col in df.columns if col not in list_of_normalization_select.keys()])}
        from dataset
    """)
    if log: pDif(ini, "2.3 Columns normalized")

    # get columns normalization that dataset has
    columns_to_transform = [
        f"{inp['input'].replace('__key__', col_name) } as {inp['output']}"
        for col_name, t in columns_master['columns_transform'].items()
        if col_name in df.columns
        for inp in t
    ]

    # get list of outputs to ignore in select comparing with df.columns
    list_of_output = [
        y['output']
        for x in columns_master['columns_transform'].values()
        for y in x
    ]

    # run transformation
    df = run_query(f"""
        select
        {','.join(columns_to_transform)},
        {','.join([c for c in df.columns if c not in list_of_output])}
        from dataset
    """)
    if log: pDif(ini, "2.4 Columns normalized")

    # go over each attribute and check in the dataset schema if the column already has the column
    # in the same dtype expected
    columns_to_cast = {
        col_name: dtype
        for col_name, dtype in columns_master['attributes'].items()
        if (
            col_name in {x: y for x, y in df.dtypes}.keys() 
            and dtype!={x: y for x, y in df.dtypes}[col_name]
        )
    }
    
    # unziping list, select normal columns and cast columns from yaml config
    df = df.select(
        *[col(c) for c in df.columns if c not in columns_to_cast.keys()], # select normal coluns
        *[ # select columns to cast
            col(c).cast(dtype).alias(c)
            for c, dtype in columns_to_cast.items()
        ])
    if log: pDif(ini, "3.1 Attributes columns converted")

    # get regext object for cleaning columns
    columns_to_apply_regex = {
        k: v
        for k, v in columns_master['validations'].items()
        if k in df.columns # see if column need to be cleaned
    }

    # apply regex validation in each column
    df = df.select( \
        *[# select normal columns
            col(c)
            for c in df.columns 
            if c not in columns_to_apply_regex.keys() ],
        *[ # select columns to validate, only keep value from column
           # if match with regex pattern
            when(col(col_name).rlike(regexson), col(col_name)) \
            .otherwise(None)
            .alias(col_name)
            for col_name, regexson in columns_to_apply_regex.items() ]
    )
    if log: pDif(ini, "4.1 Dataset Validated")

    # read filters config file
    with fs.open('cdp-becks/scripts/config/filters.yml') as yfile:
        filters = yaml.safe_load(yfile) 

    # get filters that can by applyed in the dataset
    filters_to_apply = {
        k: dic
        for k, dic in filters.items()
        if dic['tables'] in table_name
        and all(
            c in df.columns 
            for c in dic['expected_columns'].split(',')
        )
    }

    def format_ts(dt):
        return dt.strftime('%Y-%m-%d')

    # go over each filter and applyit updating the dataset
    # in each loop
    for flt in filters_to_apply.values():
        
        # params for query format
        ffrom = format_ts(flt['from'])
        fto = format_ts(flt['to'])
        flogic = flt['logic']

        # query to filter data
        df = df.filter(f"""
            ({flogic} and to_timestamp(time) between to_timestamp('{ffrom}') and to_timestamp('{fto}')) 
                or (to_timestamp(time) < to_timestamp('{ffrom}') and to_timestamp(time) > to_timestamp('{fto}')) 
        """)
    if log: pDif(ini, "4.2 Dataset filtered")
    
    # load sample csv of std
    if False:
        dataset_csv_std = spark.read.csv('s3://cdp-becks/std/mex_std.csv', header=True)
        list_of_columns_to_std = [
            col_name
            for col_name in 
            [ # loop over types of columns that need to be normalized
                r.column_name 
                for r in dataset_csv_std.select(dataset_csv_std.column_name) \
                    .distinct() \
                    .collect()
            ] if col_name in df.columns # column must be inside datasets fields
        ]

    # go over each column that need to be standarizated
    if False: #for c in list_of_columns_to_std:
        
        # slice data for just current normalization
        # rm 'time' col to avoid ambiguity
        df_s = dataset_csv_std.filter(c==col('column_name')).drop('time')
        
        # join with updated column making coalesce between new and old column
        df = df.join(df_s, df[c]==df_s['from_value'], 'left') \
        .select(
            *[col(col_name) for col_name in df.columns if col_name not in ['from_value','to_value', c]],
            coalesce(col('to_value'), col(c)).alias(c)
        )
    #if log: pDif(ini, "5.1 Dataset standardized")

    #for c in [c for c, t in df.dtypes if 'array' in t]:
    #    df = df.withColumn(c, df[c].cast('string'))
    #if log: pDif(ini, "6.1 Columns Convertion for writing")

    # record a parquet inside s3
    df.write.parquet(f"s3://{table_schema}/{table_name}")
    # if log: pDif(ini, "6.2 Dataset writed parquet in s3", record=True)

    # write in td with td-spark
    # td.create_or_replace(df, "internal.mex_web_form_from_emr")
    # if log: pDif(ini, "6.3 Dataset writed in td", record=True)

if __name__=='__main__':

    fs = s3fs.S3FileSystem(
        key='',
        secret=""
    )

    for zone in ['naz','saz']:

        table_schema = f'{zone}_external_pipeline_queue'

        list_of_tables = [
            tb.split('/')[-1] 
            for tb in fs.ls(f'/{table_schema}/') 
            if '__schema' not in tb]

        [new_base_service(table_name, table_schema, log=True)
            for table_name in list_of_tables]
