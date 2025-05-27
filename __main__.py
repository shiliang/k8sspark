import argparse
import logging
import uuid
import redis
import json
import time

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
        self.orderby_column = kwargs.get('orderby_column', '')
        self.serverip = kwargs.get('serverip', '') # Data component address
        self.serverport = kwargs.get('serverport', 0)
        # New database related parameters
        self.dbType = kwargs.get('dbType', '')
        self.host = kwargs.get('host', '')
        self.port = kwargs.get('port', 0)
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')
        self.dbName = kwargs.get('dbName', '')
        # 内置数据库
        self.embedded_dbType = kwargs.get('embedded_dbType', '')
        self.embedded_host = kwargs.get('embedded_host', '')
        self.embedded_port = kwargs.get('embedded_port', 0)
        self.embedded_username = kwargs.get('embedded_username', '')
        self.embedded_password = kwargs.get('embedded_password', '')
        self.embedded_dbName = kwargs.get('embedded_dbName', '')
        self.embedded_table = kwargs.get('embedded_table', '')
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
        self.partitions = kwargs.get('partitions', None)
        # Redis相关参数
        self.redis_host = kwargs.get('redis_host', 'localhost')
        self.redis_port = kwargs.get('redis_port', 6379)
        self.redis_password = kwargs.get('redis_password', '')
        self.redis_db = kwargs.get('redis_db', 0)
        # Pod名称参数
        self.podName = kwargs.get('podName', '')
        # 目标表
        self.target_table = kwargs.get('target_table', '')
        self.storage_type = kwargs.get('storage_type', '')


def parse_args():
    parser = argparse.ArgumentParser(description="DynamicDatabaseJob 1.0")

    # Add mode parameter
    parser.add_argument("--mode", type=str, required=True, 
                       choices=["query", "write", "sort", "count", "groupby_count", "join", "add_hash_column", "psi_join"], 
                       help="Operation mode: query, write, sort, count, groupby_count, join, add_hash_column, or psi_join")

    parser.add_argument("--accesskey", type=str, help="OSS access key")
    parser.add_argument("--secretkey", type=str, help="OSS secret key")
    parser.add_argument("--query", type=str, help="SQL query to execute")
    parser.add_argument("--bucket", type=str, required=True, help="Bucket located in OSS")
    parser.add_argument("--dataobject", type=str, help="Data object located in OSS")
    parser.add_argument("--inobjects", type=str, help="List of in params object located in OSS (comma-separated)")
    parser.add_argument("--incolumns", type=str, help="List of columns for IN clause (comma-separated)")
    parser.add_argument("--endpoint", type=str, required=True, help="Endpoint in OSS")
    parser.add_argument("--columns", type=str, help="List of columns to SELECT (comma-separated)")
    parser.add_argument("--orderby_column", type=str, help="Order by column")
    parser.add_argument("--serverip", type=str, help="Data server IP")
    parser.add_argument("--serverport", type=int, help="Data server port")
    # New database related parameters
    parser.add_argument("--dbType", type=str, choices=["1", "2"], help="Database type (1 for MySQL, 2 for KingBase)")
    parser.add_argument("--host", type=str, help="Database host address")
    parser.add_argument("--port", type=int, help="Database port")
    parser.add_argument("--username", type=str, help="Database username")
    parser.add_argument("--password", type=str, help="Database password")
    parser.add_argument("--dbName", type=str, help="Database name")
    # embedded database related parameters
    parser.add_argument("--embedded_dbType", type=str, choices=["1", "2"], help="Database type (1 for MySQL, 2 for KingBase)")
    parser.add_argument("--embedded_host", type=str, help="Database host address")
    parser.add_argument("--embedded_port", type=int, help="Database port")
    parser.add_argument("--embedded_username", type=str, help="Database username")
    parser.add_argument("--embedded_password", type=str, help="Database password")
    parser.add_argument("--embedded_dbName", type=str, help="Database name")
    parser.add_argument("--embedded_table", type=str, help="Database table name")
    # Add group by columns parameter
    parser.add_argument("--groupby_columns", type=str, help="Columns for GROUP BY clause (comma-separated)")
    # Add where condition parameter
    parser.add_argument("--where_condition", type=str, help="WHERE condition for filtering data (comma-separated)")
    # Add join related parameters
    parser.add_argument("--join_type", type=str, choices=["inner", "left", "right", "outer"], default="inner", help="Join type")
    parser.add_argument("--join_columns", type=str, help="Columns to join on (comma-separated)")
    parser.add_argument("--db_table", type=str, help="Database table name for join")
    parser.add_argument("--partitions", type=int,
                        help="Number of partitions to use (optional, will calculate if not provided)")
    # Redis相关参数
    parser.add_argument("--redis_host", type=str, default="localhost", help="Redis host")
    parser.add_argument("--redis_port", type=int, default=6379, help="Redis port")
    parser.add_argument("--redis_password", type=str, default="", help="Redis password")
    parser.add_argument("--redis_db", type=int, default=0, help="Redis database number")
    # Pod名称参数
    parser.add_argument("--podName", type=str, required=True, help="Pod name for Redis key")
    # 目标表
    parser.add_argument("--target_table", type=str, help="Target table name")
    # 存储类型
    parser.add_argument("--storage_type", type=str, choices=["db", "minio"], default="db", help="Storage type (db or minio)")

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
        elif config.mode == "psi_join":
            # 根据存储类型选择不同的PSI join实现
            if hasattr(config, 'storage_type') and config.storage_type == "minio":
                logger.info("Using MinIO storage for PSI join results")
                do_psi_join_minio(spark, config, arrow_uploader)
            else:
                logger.info("Using database storage for PSI join results")
                do_psi_join_db(spark, config, arrow_uploader)
        elif config.mode == "count":
            do_count(spark, config, arrow_uploader)
        elif config.mode == "groupby_count":
            do_groupby_count(spark, config, arrow_uploader)
        elif config.mode == "add_hash_column":
            do_add_hash_column(spark, config, arrow_uploader)
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
    object_name = save_dataframe_to_minio_distributed(result, config)
    if object_name is None:
        raise Exception("Failed to save result to MinIO")
    
    logger.info(f"DataFrame successfully uploaded to MinIO as {object_name}, row count: {result.count()}.")
    # 通知服务端
    notify_server_of_completion(config, get_job_result(config, "success"))


def do_write(spark, config, arrow_uploader):
    """Execute write operation"""
    if not config.dataobject:
        raise ValueError("Dataobject parameter is required for write mode")
    if not config.dbType:
        raise ValueError("dbType parameter is required for write mode")
    
    # 读取数据资产的表数据
    db_strategy = DatabaseFactory.get_strategy(
        config.dbType, 
        config.host, 
        config.port, 
        config.dbName, 
        config.username, 
        config.password
    )
    
    # 构建连接参数
    jdbc_properties = {
        "user": config.username,
        "password": config.password,
        "driver": db_strategy.get_driver()
    }
    
    # 设置分区数，如果未指定则默认为10
    num_partitions = config.partitions if config.partitions else 10
    
    # 取第一个join列作为分区键
    join_column = config.join_columns[0]
    
    # 创建谓词分区 - 组合使用MOD和CRC32
    predicates = []
    for i in range(num_partitions):
        # 对于最后一个分区，使用 >= 确保不漏数据
        if i == num_partitions - 1:
            predicates.append(f"MOD(ABS(CRC32({join_column})), {num_partitions}) >= {i}")
        else:
            predicates.append(f"MOD(ABS(CRC32({join_column})), {num_partitions}) = {i}")
    
    # 使用日志记录谓词
    logger.info(f"Using predicates for partitioned reading: {predicates}")
    
    # 使用谓词分区读取数据库
    db_df = spark.read.jdbc(
        url=db_strategy.get_jdbc_url(),
        table=config.db_table,
        predicates=predicates,
        properties=jdbc_properties
    )
    
    # 写入到数据库
    save_dataframe_to_embedded_database(db_df, config)
    
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
    object_name = save_dataframe_to_minio_distributed(sorted_df, config)
    if object_name is None:
        raise Exception("Failed to save sorted result to MinIO")
    
    logger.info(f"Sorted data successfully uploaded to MinIO as {object_name}")
    notify_server_of_completion(config, get_job_result(config, "success"))


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
    object_name = save_dataframe_to_minio_distributed(result_df, config)
    if object_name is None:
        raise Exception("Failed to save count result to MinIO")
    
    logger.info(f"Count result uploaded to MinIO as {object_name}")
    notify_server_of_completion(config, get_job_result(config, "success"))


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
    object_name = save_dataframe_to_minio_distributed(result_df, config)
    if object_name is None:
        raise Exception("Failed to save groupby count result to MinIO")
    
    logger.info(f"Group by count result uploaded to MinIO as {object_name}")
    notify_server_of_completion(config, get_job_result(config, "success"))

def do_join(spark, config, arrow_uploader):
    """Execute join operation between database table and MinIO file"""
    try:
        if not config.db_table:
            raise ValueError("db_table parameter is required for join mode")
        if not config.dataobject:
            raise ValueError("dataobject parameter is required for join mode")
        if not config.join_columns:
            raise ValueError("join_columns parameter is required for join mode")
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
        
        # 构建连接参数
        jdbc_properties = {
            "user": config.username,
            "password": config.password,
            "driver": db_strategy.get_driver()
        }
        
        # 设置分区数，如果未指定则默认为10
        num_partitions = config.partitions if config.partitions else 10
        
        # 取第一个join列作为分区键
        join_column = config.join_columns[0]
        
        # 创建谓词分区 - 组合使用MOD和CRC32
        predicates = []
        for i in range(num_partitions):
            # 对于最后一个分区，使用 >= 确保不漏数据
            if i == num_partitions - 1:
                predicates.append(f"MOD(ABS(CRC32({join_column})), {num_partitions}) >= {i}")
            else:
                predicates.append(f"MOD(ABS(CRC32({join_column})), {num_partitions}) = {i}")
        
        # 使用日志记录谓词
        logger.info(f"Using predicates for partitioned reading: {predicates}")
        
        # 使用谓词分区读取数据库
        db_df = spark.read.jdbc(
            url=db_strategy.get_jdbc_url(),
            table=config.db_table,
            predicates=predicates,
            properties=jdbc_properties
        )
        
        # 读取MinIO文件
        minio_df = read_dataframe_from_minio(arrow_uploader, spark, config.bucket, config.dataobject)
        
        # 执行join操作
        if len(config.join_columns) == 1:
            result_df = db_df.join(minio_df, on=config.join_columns[0], how=config.join_type)
        else:
            result_df = db_df.join(minio_df, on=config.join_columns, how=config.join_type)
        
        # 保存结果到中间数据库
        target_table = save_dataframe_to_embedded_database(result_df, config)
        if target_table is None:
            raise Exception("Failed to save join result to database")
        
        notify_server_of_completion(config, get_job_result(config, "success"))
        
    except Exception as e:
        logger.error(f"Error in join operation: {str(e)}")
        notify_server_of_completion(config, get_job_result(config, "error", str(e)))
        raise

def do_psi_join_db(spark, config, arrow_uploader):
    """Execute psi join operation"""
    try:
         # 总体开始时间
        total_start_time = time.time()

        if not config.dataobject:
            raise ValueError("dataobject parameter is required for psi join mode")
        if not config.join_columns:
            raise ValueError("join_columns parameter is required for psi join mode")
        if not config.orderby_column:
            raise ValueError("orderby_column parameter is required for psi join mode")
        
        # 从中间表中取数据
        db_strategy = DatabaseFactory.get_strategy(
            config.embedded_dbType, 
            config.embedded_host, 
            config.embedded_port, 
            config.embedded_dbName, 
            config.embedded_username, 
            config.embedded_password
        )
        
        # 构建连接参数
        jdbc_properties = {
            "user": config.embedded_username,
            "password": config.embedded_password,
            "driver": db_strategy.get_driver()
        }
        
        # 设置分区数，如果未指定则默认为10
        num_partitions = config.partitions if config.partitions else 10
        
        # 取第一个join列作为分区键
        join_column = config.join_columns[0]
        
        # 创建谓词分区 - 组合使用MOD和CRC32
        predicates = []
        for i in range(num_partitions):
            # 对于最后一个分区，使用 >= 确保不漏数据
            if i == num_partitions - 1:
                predicates.append(f"MOD(ABS(CRC32({join_column})), {num_partitions}) >= {i}")
            else:
                predicates.append(f"MOD(ABS(CRC32({join_column})), {num_partitions}) = {i}")
        
        # 使用日志记录谓词
        logger.info(f"Using predicates for partitioned reading: {predicates}")

        # 使用优化的批量读取方法
        logger.info(f"Reading {len(config.inobjects)} Arrow files using optimized method")
        db_df = arrow_uploader.read_arrow_files_as_partitions_optimized(
            spark=spark,
            config=config
        )
        
        # 预分区并缓存
        db_df = db_df.repartition(num_partitions, join_column)
        db_df.cache()
        
        # 读取MinIO文件
        minio_df = read_dataframe_from_minio(arrow_uploader, spark, config.bucket, config.dataobject)
        
        # 执行join操作
        if len(config.join_columns) == 1:
            result_df = db_df.join(minio_df, on=config.join_columns[0], how=config.join_type)
        else:
            result_df = db_df.join(minio_df, on=config.join_columns, how=config.join_type)

        # 按照orderby_column进行升序排序
        result_df = result_df.sort(config.orderby_column) 
        
        # 保存结果到中间数据库
        target_table = save_dataframe_to_target_table(result_df, config)
        if target_table is None:
            raise Exception("Failed to save join result to database")
        
        # 总耗时统计
        total_time = time.time() - total_start_time
        logger.info(f"Total PSI join time: {total_time:.2f} seconds")
        
        save_result_to_redis(config, get_job_result(config, "success"))
        
    except Exception as e:
        logger.error(f"Error in join operation: {str(e)}")
        save_result_to_redis(config, get_job_result(config, "error", str(e)))
        raise

def do_psi_join_minio(spark, config, arrow_uploader):
    """Execute psi join operation - optimized version"""
    try:
        total_start_time = time.time()
        
        # 验证参数
        if not config.dataobject:
            raise ValueError("dataobject parameter is required for psi join mode")
        if not config.join_columns:
            raise ValueError("join_columns parameter is required for psi join mode")
        if not config.orderby_column:
            raise ValueError("orderby_column parameter is required for psi join mode")
        if not config.inobjects:
            raise ValueError("inobjects parameter is required for reading from MinIO")
        
        num_partitions = config.partitions if config.partitions else 10
        join_column = config.join_columns[0]
        
        # 使用优化的批量读取方法
        load_start_time = time.time()
        logger.info(f"Reading {len(config.inobjects)} Arrow files using optimized method")
        db_df = arrow_uploader.read_arrow_files_as_partitions_optimized(
            spark=spark,
            config=config
        )
        
        # 预分区并缓存
        # db_df = db_df.repartition(num_partitions, join_column)
        db_df.cache()

        load_time = time.time() - load_start_time
        logger.info(f"Data loading time: {load_time:.2f} seconds")
        
        # 优化2: 读取第二个数据源并缓存
        minio_df = read_dataframe_from_minio(arrow_uploader, spark, config.bucket, config.dataobject)
        minio_df.cache()
        
        # 检查是否可以使用广播join
        minio_count = minio_df.count()
        
        join_start_time = time.time()
        
        # 优化3: 智能选择join策略
        if minio_count < 1000000:  # 小表使用广播join
            from pyspark.sql.functions import broadcast
            logger.info(f"Using broadcast join for small table ({minio_count} rows)")
            result_df = db_df.join(broadcast(minio_df), on=join_column, how=config.join_type)
        else:
            # 大表时确保分区对齐
            logger.info(f"Using partitioned join for large table ({minio_count} rows)")
            minio_df = minio_df.repartition(num_partitions, join_column)
            result_df = db_df.join(minio_df, on=join_column, how=config.join_type)
        
        # 排序
        result_df = result_df.sort(config.orderby_column)
        
        join_time = time.time() - join_start_time
        logger.info(f"Join operation time: {join_time:.2f} seconds")
        
        # 保存结果
        save_start_time = time.time()
        target_table = save_dataframe_to_target_table(result_df, config)
        if target_table is None:
            raise Exception("Failed to save join result to database")
        
        save_time = time.time() - save_start_time
        logger.info(f"Save operation time: {save_time:.2f} seconds")
        
        # 清理缓存
        db_df.unpersist()
        minio_df.unpersist()
        
        total_time = time.time() - total_start_time
        logger.info(f"Total PSI join time: {total_time:.2f} seconds")
        
        save_result_to_redis(config, get_job_result(config, "success"))
        
    except Exception as e:
        logger.error(f"Error in join operation: {str(e)}")
        save_result_to_redis(config, get_job_result(config, "error", str(e)))
        raise

def do_add_hash_column(spark, config, arrow_uploader):
    """Execute add hash column operation"""
    try:
        if not config.db_table:
            raise ValueError("db_table parameter is required for add_hash_column mode")
        if not config.join_columns:
            raise ValueError("join_columns parameter is required for add_hash_column mode")
        if not config.dbType:
            raise ValueError("dbType parameter is required for add_hash_column mode")
        if not config.embedded_table:
            raise ValueError("embedded_table parameter is required for add_hash_column mode")
        
        # 从数据资产中加载数据到内存
        db_strategy = DatabaseFactory.get_strategy(
            config.dbType, 
            config.host, 
            config.port, 
            config.dbName, 
            config.username, 
            config.password
        )
        
        # 构建连接参数
        jdbc_properties = {
            "user": config.username,
            "password": config.password,
            "driver": db_strategy.get_driver()
        }
        
        # 设置分区数，如果未指定则默认为10
        num_partitions = config.partitions if config.partitions else 10
        
        # 取第一个join列作为分区键
        join_column = config.join_columns[0]
        
        # 创建谓词分区
        predicates = []
        for i in range(num_partitions):
            if i == num_partitions - 1:
                predicates.append(f"MOD(ABS(CRC32({join_column})), {num_partitions}) >= {i}")
            else:
                predicates.append(f"MOD(ABS(CRC32({join_column})), {num_partitions}) = {i}")
        
        logger.info(f"Using predicates for partitioned reading: {predicates}")
        
        # 读取数据库数据
        db_df = spark.read.jdbc(
            url=db_strategy.get_jdbc_url(),
            table=config.db_table,
            predicates=predicates,
            properties=jdbc_properties
        )
        
        # 缓存数据
        db_df.cache()
        db_df.count()  # 触发缓存
        
        # 添加hash列
        from pyspark.sql.functions import sha2, concat_ws, col, substring
        
        result_df = db_df.withColumn(
            "combined_join_column",
            substring(
                sha2(
                    concat_ws("|", *[col(c) for c in config.join_columns]),
                    256
                ),
                1, 32
            )
        )
        
        total_start_time = time.time()

        if config.storage_type == "db":
            db_start_time = time.time()
            table_name = save_dataframe_to_embedded_database(result_df, config)
            db_time = time.time() - db_start_time
            logger.info(f"保存到数据库耗时: {db_time:.2f}秒, 表名: {table_name}")

            if table_name is None:
                raise Exception("Failed to save result to database")

        elif config.storage_type == "minio":
            minio_start_time = time.time()
            file_list = save_dataframe_to_minio_distributed(result_df, config)
            minio_time = time.time() - minio_start_time
            logger.info(f"保存到MinIO耗时: {minio_time:.2f}秒, 文件数: {len(file_list)}")

            if not file_list:
                raise Exception("Failed to save result to MinIO")

        else:
            raise Exception(f"Unknown storage_type: {config.storage_type}")

        # 保存hash列到csv文件
        csv_start_time = time.time()
        object_name = arrow_uploader.save_hash_column_to_csv(result_df, "combined_join_column")
        csv_time = time.time() - csv_start_time
        logger.info(f"保存hash列耗时: {csv_time:.2f}秒, CSV文件名: {object_name}")

        if object_name is None:
            raise Exception("Failed to save hash column to csv file")

        # 总耗时统计
        total_time = time.time() - total_start_time
        logger.info(f"总保存耗时: {total_time:.2f}秒")

        # 生成作业结果
        result = get_job_result(config, "success", data={
            "bucket": config.bucket,
            "object": object_name,
            "arrow_files": file_list  # 包含所有Arrow文件信息
        })

        # 保存到Redis
        save_result_to_redis(config, result)
        
    except Exception as e:
        logger.error(f"Error in add_hash_column operation: {str(e)}")
        result = get_job_result(config, "error", str(e))
        save_result_to_redis(config, result)
        raise
    finally:
        # 释放缓存
        if 'db_df' in locals():
            db_df.unpersist()

def get_job_result(config, status="success", error=None, data=None):
    """
    统一的作业结果返回格式
    
    Args:
        config: 配置对象
        status: 状态 ("success" 或 "error")
        error: 错误信息（如果有）
        data: 包含所有其他数据的字典（如dbName, tableName, objectName等）
    
    Returns:
        dict: 统一格式的结果
    """
    result = {
        "status": status,
        "mode": config.mode
    }
    
    if error:
        result["error"] = error
    
    if data:
        result.update(data)
        
    return result            

def notify_server_of_completion(config, result):
    server_endpoint = f"{config.serverip}:{config.serverport}"
    url = urljoin(f"http://{server_endpoint}", "/api/job/completed")
    logger.info(f"Notifying server with result: {result}")

    try:
        response = requests.post(url, json=result, headers={'Content-Type': 'application/json'})
        logger.info(f"Server notification response: {response.text}")
    except Exception as e:
        logger.error(f"Failed to notify server: {e}")

def save_result_to_redis(config, result):
    """
    将作业结果保存到Redis
    
    Args:
        config: 配置对象
        result: 作业结果
    """
    
    try:
        # 连接Redis
        redis_client = redis.Redis(
            host=config.redis_host, 
            port=config.redis_port,
            password=config.redis_password,
            db=config.redis_db
        )
        
        # 使用podName作为key
        key = f"job_result:{config.podName}"
        
        # 将结果转为JSON字符串
        result_json = json.dumps(result)
        
        # 保存到Redis，设置过期时间为1天
        redis_client.setex(key, 86400, result_json)
        
        # 记录Redis信息
        logger.info(f"Result saved to Redis with key: {key}")
        
        return True
    except Exception as e:
        logger.error(f"Failed to save result to Redis: {e}")
        return False         


def save_dataframe_to_minio_distributed(result, config):
    """将DataFrame的每个分区保存为独立的Arrow文件到MinIO的data/目录下"""
    # 检查分区数
    current_partitions = result.rdd.getNumPartitions()
    logger.info(f"保存到MinIO的DataFrame分区数: {current_partitions}")
    
    # 获取DataFrame的列名
    column_names = result.columns
    logger.info(f"DataFrame列名: {column_names}")
    
    # 从podName中提取request.RequestId
    request_id = config.podName.replace("spark-job-", "")
    data_directory = f"data/{request_id}/"
    
    # 生成唯一标识前缀
    unique_id = uuid.uuid4().hex
    result_prefix = f"{data_directory}result_{unique_id}_part"
    
    def process_and_save_partition(partition_id, iterator):
        import pandas as pd
        import pyarrow as pa
        from src.arrow_uploader import ArrowUploader
        
        # 在每个分区中创建上传器
        part_uploader = ArrowUploader(
            config.endpoint, config.accesskey, config.secretkey, config.bucket
        )
        
        # 处理分区数据
        rows = list(iterator)
        logger.info(f"分区 {partition_id} 包含 {len(rows)} 行数据")
        
        if not rows:
            logger.info(f"分区 {partition_id} 为空")
            return iter([])
        
        # 使用原始列名创建DataFrame
        pdf = pd.DataFrame(rows, columns=column_names)
        if pdf.empty:
            logger.info(f"分区 {partition_id} 转换为DataFrame后为空")
            return iter([])
            
        # 验证列名
        logger.info(f"分区 {partition_id} DataFrame列名: {pdf.columns.tolist()}")
        
        # 转换为Arrow表
        table = pa.Table.from_pandas(pdf)
        
        # 验证Arrow表的schema
        logger.info(f"分区 {partition_id} Arrow表schema: {table.schema}")
        
        # 最终文件命名
        file_name = f"{result_prefix}{partition_id}.arrow"
        
        # 使用save_to_minio直接保存最终文件
        part_uploader.save_to_minio(table, file_name)
        logger.info(f"分区 {partition_id} 保存文件: {file_name}")
        
        return iter([file_name])
    
    # 分布式处理并直接保存最终文件
    file_names = result.rdd.mapPartitionsWithIndex(
        process_and_save_partition
    ).collect()
    
    # 过滤掉空值
    valid_files = [f for f in file_names if f]
    
    logger.info(f"有效文件数量: {len(valid_files)}")
    
    if not valid_files:
        logger.warning("No data was saved - result DataFrame may be empty")
        return None
    
    # 记录所有保存的文件名，便于追踪
    logger.info(f"Saved {len(valid_files)} Arrow files with prefix {result_prefix}")
    
    return valid_files

def save_dataframe_to_embedded_database(result, config):
    """
    将Spark DataFrame保存到内置数据库（如SQLite、H2、Derby、HSQLDB）

    Args:
        result: Spark DataFrame
        config: 配置对象，需包含embedded_dbType、embedded_dbName等参数

    Returns:
        str: 实际写入的表名
    """
    # 参数校验
    if not config.embedded_dbType:
        raise ValueError("embedded_dbType 参数不能为空")
    if not config.embedded_dbName:
        raise ValueError("embedded_dbName 参数不能为空")

    # 表名
    target_table = config.embedded_table
    if not target_table:
        import uuid
        target_table = f"result_{uuid.uuid4().hex[:8]}"
        logger.info(f"未指定表名，自动生成表名: {target_table}")

    # 获取数据库驱动和URL
    db_strategy = DatabaseFactory.get_strategy(
        config.embedded_dbType, 
        config.embedded_host, 
        config.embedded_port, 
        config.embedded_dbName, 
        config.embedded_username, 
        config.embedded_password
    )
    jdbc_url = db_strategy.get_jdbc_url()

    # 构建连接参数
    jdbc_properties = {
        "driver": db_strategy.get_driver()
    }
    if config.embedded_username:
        jdbc_properties["user"] = config.embedded_username
    if config.embedded_password:
        jdbc_properties["password"] = config.embedded_password

    # 写入模式
    write_mode = "overwrite"  # 也可以根据需要改为"append"

    try:
        logger.info(f"写入DataFrame到内置数据库表: {target_table}")
        logger.info(f"JDBC URL: {jdbc_url}")
        logger.info(f"JDBC属性: {jdbc_properties}")

        result.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", target_table) \
            .mode(write_mode) \
            .options(**jdbc_properties) \
            .save()

        row_count = result.count()
        logger.info(f"成功写入 {row_count} 行到表 {target_table}")
        return target_table
    except Exception as e:
        logger.error(f"写入内置数据库失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None
    
def save_dataframe_to_target_table(result, config):
    # 参数校验
    if not config.embedded_dbType:
        raise ValueError("embedded_dbType 参数不能为空")
    if not config.embedded_dbName:
        raise ValueError("embedded_dbName 参数不能为空")

    # 表名
    target_table = config.target_table
    if not target_table:
        import uuid
        target_table = f"result_{uuid.uuid4().hex[:8]}"
        logger.info(f"未指定表名，自动生成表名: {target_table}")

    # 获取数据库驱动和URL
    db_strategy = DatabaseFactory.get_strategy(
        config.embedded_dbType, 
        config.embedded_host, 
        config.embedded_port, 
        config.embedded_dbName, 
        config.embedded_username, 
        config.embedded_password
    )
    jdbc_url = db_strategy.get_jdbc_url()

    # 构建连接参数
    jdbc_properties = {
        "driver": db_strategy.get_driver()
    }
    if config.embedded_username:
        jdbc_properties["user"] = config.embedded_username
    if config.embedded_password:
        jdbc_properties["password"] = config.embedded_password

    try:
        # 1. 直接写入目标表，但先不设置主键
        result.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", target_table) \
            .mode("overwrite") \
            .options(**jdbc_properties) \
            .save()
            
        # 2. 使用JDBC执行ALTER TABLE
        import mysql.connector
        
        # 创建MySQL连接
        conn = mysql.connector.connect(
            host=config.embedded_host,
            port=config.embedded_port,
            user=config.embedded_username,
            password=config.embedded_password,
            database=config.embedded_dbName
        )
        
        cursor = conn.cursor()
        
        try:
            # 添加主键约束
            alter_table_sql = f"""
            ALTER TABLE {target_table} 
            ADD PRIMARY KEY ({config.orderby_column})
            """
            
            cursor.execute(alter_table_sql)
            conn.commit()
            
            logger.info(f"成功创建表 {target_table} 并将 {config.orderby_column} 设置为主键")
            return target_table
            
        finally:
            cursor.close()
            conn.close()
        
    except Exception as e:
        logger.error(f"写入数据库失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
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
