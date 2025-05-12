import argparse
import logging
import uuid

import requests
import pyarrow as pa
from pyspark.sql import SparkSession
from urllib.parse import urljoin
from pyspark.sql.functions import expr

from src.arrow_uploader import ArrowUploader
from src.DatabaseStrategy import DatabaseFactory

# 初始化日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Config:
    def __init__(self, **kwargs):
        self.accesskey = kwargs.get('accesskey', '')
        self.secretkey = kwargs.get('secretkey', '')
        self.bucket = kwargs.get('bucket', '')
        self.dataobject = kwargs.get('dataobject', '')
        self.inobjects = kwargs.get('inobjects', []) # Data files for where xxx in fields
        self.query = kwargs.get('query', '')
        self.endpoint = kwargs.get('endpoint', '')
        self.incolumns = kwargs.get('incolumns', []) # Fields for where xxx in clause
        self.columns = kwargs.get('columns', []) # Fields to select
        self.serverip = kwargs.get('serverip', '') # Data component address
        self.serverport = kwargs.get('serverport', 0)
        # New database related parameters
        self.dbType = kwargs.get('dbType', '')
        self.host = kwargs.get('host', '')
        self.port = kwargs.get('port', 0)
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')
        self.dbName = kwargs.get('dbName', '')
        # Add mode parameter
        self.mode = kwargs.get('mode', 'query')  # Default to query mode
        # Add group by columns parameter
        self.groupby_columns = kwargs.get('groupby_columns', []) # 已经是数组
        # Add where condition parameter
        self.where_condition = kwargs.get('where_condition', []) # 改为数组
        # Add join related parameters
        self.join_type = kwargs.get('join_type', 'inner')
        self.join_columns = kwargs.get('join_columns', [])
        self.db_table = kwargs.get('db_table', '')


def parse_args():
    parser = argparse.ArgumentParser(description="DynamicDatabaseJob 1.0")

    # Add mode parameter
    parser.add_argument("--mode", type=str, required=True, 
                       choices=["query", "write", "sort", "count", "groupby_count", "join"], 
                       help="Operation mode: query, write, sort, count, groupby_count, or join")

    parser.add_argument("--accesskey", type=str, help="OSS access key")
    parser.add_argument("--secretkey", type=str, help="OSS secret key")
    parser.add_argument("--query", type=str, help="SQL query to execute")
    parser.add_argument("--bucket", type=str, required=True, help="Bucket located in OSS")
    parser.add_argument("--dataobject", type=str, help="Data object located in OSS")
    parser.add_argument("--inobjects", type=str, help="List of in params object located in OSS (comma-separated)")
    parser.add_argument("--incolumns", type=str, help="List of columns for IN clause (comma-separated)")
    parser.add_argument("--endpoint", type=str, required=True, help="Endpoint in OSS")
    parser.add_argument("--columns", type=str, help="List of columns to SELECT (comma-separated)")
    parser.add_argument("--serverip", type=str, help="Data server IP")
    parser.add_argument("--serverport", type=int, help="Data server port")
    # New database related parameters
    parser.add_argument("--dbType", type=str, choices=["1", "2"], help="Database type (1 for MySQL, 2 for KingBase)")
    parser.add_argument("--host", type=str, help="Database host address")
    parser.add_argument("--port", type=int, help="Database port")
    parser.add_argument("--username", type=str, help="Database username")
    parser.add_argument("--password", type=str, help="Database password")
    parser.add_argument("--dbName", type=str, help="Database name")
    # Add group by columns parameter
    parser.add_argument("--groupby_columns", type=str, help="Columns for GROUP BY clause (comma-separated)")
    # Add where condition parameter
    parser.add_argument("--where_condition", type=str, help="WHERE condition for filtering data (comma-separated)")
    # Add join related parameters
    parser.add_argument("--join_type", type=str, choices=["inner", "left", "right", "outer"], default="inner", help="Join type")
    parser.add_argument("--join_columns", type=str, help="Columns to join on (comma-separated)")
    parser.add_argument("--db_table", type=str, help="Database table name for join")

    args = parser.parse_args()
    
    # Convert comma-separated strings to lists
    if args.inobjects:
        args.inobjects = args.inobjects.split(',')
    if args.incolumns:
        args.incolumns = args.incolumns.split(',')
    if args.columns:
        args.columns = args.columns.split(',')
    if args.groupby_columns:
        args.groupby_columns = args.groupby_columns.split(',')
    if args.where_condition:
        args.where_condition = args.where_condition.split(',')
    if args.join_columns:
        args.join_columns = args.join_columns.split(',')
    
    return Config(**vars(args))


def run_spark_job(config):
    spark = SparkSession.builder \
        .appName("DynamicDatabaseJob") \
        .config("spark.hadoop.fs.s3a.access.key", config.accesskey) \
        .config("spark.hadoop.fs.s3a.secret.key", config.secretkey) \
        .config("spark.hadoop.fs.s3a.endpoint", config.endpoint) \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    arrow_uploader = ArrowUploader(config.endpoint, config.accesskey, config.secretkey, config.bucket)

    try:
        if config.mode == "query":
            do_query(spark, config, arrow_uploader)
        elif config.mode == "write":
            do_write(spark, config, arrow_uploader)
        elif config.mode == "sort":
            do_sort(spark, config, arrow_uploader)
        elif config.mode == "join":
            do_join(spark, config, arrow_uploader)
        elif config.mode == "count":
            do_count(spark, config, arrow_uploader)
        elif config.mode == "groupby_count":
            do_groupby_count(spark, config, arrow_uploader)
        else:
            logger.error(f"Unknown mode: {config.mode}")
    except Exception as e:
        logger.error(f"Error in {config.mode} operation: {e}")
    finally:
        spark.stop()


def do_query(spark, config, arrow_uploader):
    """Execute query operation"""
    if not config.query:
        raise ValueError("Query parameter is required for query mode")
    
    # 读取主数据
    df = read_dataframe_from_minio(arrow_uploader, spark, config.bucket, config.dataobject)
    # 过滤字段数据
    filtered_df = df

    # 确保 config.incolumns 和 config.inobjects 不为 None
    if config.incolumns is not None and config.inobjects is not None:
        # 确保 incolumns 和 inobjects 的长度一致
        if len(config.incolumns) == len(config.inobjects):
            for col, inobject in zip(config.incolumns, config.inobjects):
                filter_df = read_dataframe_from_minio(arrow_uploader, spark, config.bucket, inobject)
                # 只选择需要过滤的列
                filter_df = filter_df.select(col)

                # 对主数据进行 JOIN
                filtered_df = filtered_df.join(filter_df, on=col, how="inner")
                logger.info(f"Performed join on column {col}")
        else:
            logger.error("Length of incolumns and inobjects do not match.")
    else:
        logger.warning("incolumns or inobjects is None, skipping join operation.")

    # 注册为临时视图
    filtered_df.createOrReplaceTempView("filtered_table")
    # 动态构建 SELECT 查询
    if config.columns:
        # 如果有指定的列，则用指定列生成查询
        select_columns = ", ".join(config.columns)
    else:
        # 如果没有指定列，默认使用 SELECT *
        select_columns = "*"

    # 构建完整的查询语句
    query = f"SELECT {select_columns} FROM filtered_table"

    # 执行 SQL 查询
    logger.info(f"Executing SQL query: {query}")
    result = spark.sql(query)

    # 将查询结果上传到 MinIO
    object_name = save_dataframe_to_minio(result, config, arrow_uploader)
    logger.info(f"DataFrame successfully uploaded to MinIO as {object_name}, row count: {result.count()}.")
    # 通知服务端
    notify_server_of_completion(config, object_name, result.count())


def do_write(spark, config, arrow_uploader):
    """Execute write operation"""
    if not config.dataobject:
        raise ValueError("Dataobject parameter is required for write mode")
    if not config.dbType:
        raise ValueError("dbType parameter is required for write mode")
    
    # 读取数据
    df = read_dataframe_from_minio(arrow_uploader, spark, config.bucket, config.dataobject)
    
    # 获取数据库策略
    db_strategy = DatabaseFactory.get_strategy(
        config.dbType, 
        config.host, 
        config.port, 
        config.dbName, 
        config.username, 
        config.password
    )
    
    # 写入到数据库
    df.write \
        .format("jdbc") \
        .option("url", db_strategy.get_jdbc_url()) \
        .option("driver", db_strategy.get_driver()) \
        .option("dbtable", "target_table") \
        .option("user", config.username) \
        .option("password", config.password) \
        .mode("append") \
        .save()
    
    logger.info("Data successfully written to database")


def do_sort(spark, config, arrow_uploader):
    """Execute sort operation"""
    if not config.dataobject:
        raise ValueError("Dataobject parameter is required for sort mode")
    if not config.columns:
        raise ValueError("Columns parameter is required for sort mode")
    
    # 读取数据
    df = read_dataframe_from_minio(arrow_uploader, spark, config.bucket, config.dataobject)
    
    # 排序
    sorted_df = df.sort(*config.columns)
    
    # 保存排序结果
    object_name = save_dataframe_to_minio(sorted_df, config, arrow_uploader)
    logger.info(f"Sorted data successfully uploaded to MinIO as {object_name}")
    notify_server_of_completion(config, object_name, sorted_df.count())


def do_count(spark, config, arrow_uploader):
    """Execute count operation"""
    if not config.dataobject:
        raise ValueError("Dataobject parameter is required for count mode")
    
    # 读取数据
    df = read_dataframe_from_minio(arrow_uploader, spark, config.bucket, config.dataobject)
    
    # 应用where条件
    if config.where_condition:
        df = df.filter(config.where_condition)
        logger.info(f"Applied WHERE condition: {config.where_condition}")
    
    # 统计总行数
    row_count = df.count()
    logger.info(f"Total row count: {row_count}")
    
    # 将结果保存为单行DataFrame
    result_df = spark.createDataFrame([(row_count,)], ["count"])
    
    # 保存结果到MinIO
    object_name = save_dataframe_to_minio(result_df, config, arrow_uploader)
    logger.info(f"Count result uploaded to MinIO as {object_name}")
    notify_server_of_completion(config, object_name, 1)  # 1 row in result


def do_groupby_count(spark, config, arrow_uploader):
    """Execute group by count operation"""
    if not config.dataobject:
        raise ValueError("Dataobject parameter is required for groupby_count mode")
    if not config.groupby_columns:
        raise ValueError("groupby_columns parameter is required for groupby_count mode")
    
    # 读取数据
    df = read_dataframe_from_minio(arrow_uploader, spark, config.bucket, config.dataobject)
    
    # 应用where条件
    if config.where_condition:
        df = df.filter(config.where_condition)
        logger.info(f"Applied WHERE condition: {config.where_condition}")
    
    # 执行group by count
    result_df = df.groupBy(*config.groupby_columns).count()
    
    # 保存结果到MinIO
    object_name = save_dataframe_to_minio(result_df, config, arrow_uploader)
    logger.info(f"Group by count result uploaded to MinIO as {object_name}")
    notify_server_of_completion(config, object_name, result_df.count())

def do_join(spark, config, arrow_uploader):
    """Execute join operation between database table and MinIO file"""
    if not config.db_table:
        raise ValueError("db_table parameter is required for join mode")
    if not config.dataobject:
        raise ValueError("dataobject parameter is required for join mode")
    if not config.oncolumns:
        raise ValueError("oncolumns parameter is required for join mode")
    if not config.dbType:
        raise ValueError("dbType parameter is required for join mode")
    
    # 获取数据库驱动和URL
    db_strategy = DatabaseFactory.get_strategy(
        config.dbType, 
        config.host, 
        config.port, 
        config.dbName, 
        config.username, 
        config.password
    )
    
    # 读取数据库表
    db_df = spark.read \
        .format("jdbc") \
        .option("url", db_strategy.get_jdbc_url()) \
        .option("driver", db_strategy.get_driver()) \
        .option("dbtable", config.db_table) \
        .option("user", config.username) \
        .option("password", config.password) \
        .load()
    
    # 读取MinIO文件
    minio_df = read_dataframe_from_minio(arrow_uploader, spark, config.bucket, config.dataobject)
    
    # 执行join
    join_condition = " AND ".join([f"db_df.{col} = minio_df.{col}" for col in config.join_columns])
    result_df = db_df.join(minio_df, expr(join_condition), config.join_type)
    
    # 保存结果到MinIO
    object_name = save_dataframe_to_minio(result_df, config, arrow_uploader)
    logger.info(f"Join result uploaded to MinIO as {object_name}")
    notify_server_of_completion(config, object_name, result_df.count())

def notify_server_of_completion(config, object_name, total_rows):
    server_endpoint = f"{config.serverip}:{config.serverport}"
    url = urljoin(f"http://{server_endpoint}", "/api/job/completed")
    payload = {
        "bucket": config.bucket,
        "object": object_name,
        "totalRows": total_rows
    }
    logger.info(f"Partition URLs and total rows to notify server: {payload}")
    try:
        response = requests.post(url, json=payload, headers={'Content-Type': 'application/json'})
        logger.info(f"Server notification response: {response.text}")
    except Exception as e:
        logger.error(f"Failed to notify server: {e}")


def save_dataframe_to_minio(result, config, arrow_uploader):
    """
    Convert Spark DataFrame to Arrow table and upload to MinIO.

    Args:
        result (DataFrame): Spark SQL query result DataFrame.
        config (Config): Configuration object.
        arrow_uploader (ArrowUploader): ArrowUploader instance for uploading to MinIO.

    Returns:
        str: Object name uploaded to MinIO.
    """
    try:
        # Convert result DataFrame to Arrow table
        logger.info("Converting query result to Arrow Table")
        result_arrow = pa.Table.from_pandas(result.toPandas())  # Convert Spark DataFrame to Pandas DataFrame, then to Arrow table

        # Construct filename
        unique_id = uuid.uuid4().hex
        object_name = f"result_{unique_id}.arrow"
        logger.info(f"Constructed object name: {object_name}")

        # Upload Arrow table to MinIO
        arrow_uploader.save_to_minio(result_arrow, object_name)

        # Return object name
        return object_name
    except Exception as e:
        logger.error(f"Error during Arrow Table conversion or upload to MinIO: {e}")
        return None


def read_dataframe_from_minio(arrow_uploader, spark, bucket, object):
    return arrow_uploader.read_from_minio(spark, bucket, object)


def get_data_size_gb(spark, config):
    """Estimate data size, return size in GB"""
    # 获取数据库策略
    db_strategy = DatabaseFactory.get_strategy(
        config.dbType, 
        config.host, 
        config.port, 
        config.dbName, 
        config.username, 
        config.password
    )
    
    # Read data to estimate size
    df = spark.read \
        .format("jdbc") \
        .option("url", db_strategy.get_jdbc_url()) \
        .option("driver", db_strategy.get_driver()) \
        .option("dbtable", f"({config.query.strip(';')}) AS subquery") \
        .load()

    # Calculate data size
    total_size_bytes = df.rdd.map(lambda row: len(str(row))).sum()  # Estimate bytes per row
    total_size_gb = total_size_bytes / (1024 ** 3)  # Convert to GB
    return total_size_gb


if __name__ == "__main__":
    config = parse_args()
    run_spark_job(config)
