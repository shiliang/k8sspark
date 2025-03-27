import argparse
import logging
import uuid

import requests
import pyarrow as pa
from pyspark.sql import SparkSession
from urllib.parse import urljoin

from src.DatabaseStrategy import DatabaseFactory
from src.arrow_uploader import ArrowUploader

# 初始化日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Config:
    def __init__(self, **kwargs):
        self.accesskey = kwargs.get('accesskey', '')
        self.secretkey = kwargs.get('secretkey', '')
        self.bucket = kwargs.get('bucket', '')
        self.dataobject = kwargs.get('dataobject', '')
        self.inobject = kwargs.get('inobjects', '') # where xxx in字段对应的数据文件
        self.query = kwargs.get('query', '')
        self.endpoint = kwargs.get('endpoint', '')
        self.incolumn = kwargs.get('incolumns', []) # where xxx in字段
        self.columns = kwargs.get('columns', []) # select字段
        self.serverip = kwargs.get('serverip', '') # 数据组件的地址
        self.serverport = kwargs.get('serverport', 0)


def parse_args():
    parser = argparse.ArgumentParser(description="DynamicDatabaseJob 1.0")

    parser.add_argument("--accesskey", type= str, help="OSS accesskey")
    parser.add_argument("--secretkey", type= str, help="OSS secretkey")
    parser.add_argument("--query", type= str, required=True, help="SQL query to execute")
    parser.add_argument("--bucket", type=str, required=True, help="bucket located in OSS")
    parser.add_argument("--dataobject", type= str, required=True, help="data object located in OSS")
    parser.add_argument("--inobjects", type=str, nargs='+', required=True, help="List of in params object located in OSS")
    parser.add_argument("--incolumns", type=str, nargs='+', required=True, help="List of columns for IN clause")
    parser.add_argument("--endpoint", type=str, required=True, help="endpoint in OSS")
    parser.add_argument("--columns", type=str, nargs='+', required=True, help="List of columns to SELECT")
    parser.add_argument("--serverip", type=str, help="Data server IP")
    parser.add_argument("--serverport", type=int, help="Data server port")

    args = parser.parse_args()
    return Config(**vars(args))


def run_spark_job(config):
    spark = SparkSession.builder \
        .appName("Minio Arrow Example") \
        .config("spark.hadoop.fs.s3a.access.key", config.accesskey) \
        .config("spark.hadoop.fs.s3a.secret.key", config.secretkey) \
        .config("spark.hadoop.fs.s3a.endpoint", config.endpoint) \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    try:
        # 读取主数据
        file_path = f"s3a://{config.bucket}/{config.dataobject}"
        logger.info(f"read file path: {file_path}")
        df = spark.read.format("arrow").load(file_path)

        # 过滤字段数据
        filtered_df = df

        for col, inobject in zip(config.incolumns, config.inobjects):
            filter_path = f"s3a://{config.bucket}/{inobject}"
            logger.info(f"Reading filter data for column {col} from: {filter_path}")
            filter_df = spark.read.format("arrow").load(filter_path)

            # 只选择需要过滤的列
            filter_df = filter_df.select(col)

            # 对主数据进行 JOIN
            filtered_df = filtered_df.join(filter_df, on=col, how="inner")
            logger.info(f"Performed join on column {col}")

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

        # 将结果上传到minio
        arrow_uploader = ArrowUploader(config.endpoint, config.accesskey, config.secretkey, config.bucket)
        logger.info(f"Initialized ArrowUploader with endpoint: {config.endpoint}, bucket: {config.bucket}")
        # 将查询结果上传到 MinIO
        object_name = save_dataframe_to_minio(result, config, arrow_uploader)

        # 通知服务端
        notify_server_of_completion(config, object_name, result.count())
    except Exception as e:
        logger.error(f"Error writing DataFrame to MinIO: {e}")
    finally:
        spark.stop()

def process_partition(partition, config, minio_bucket, batch_size=10):
    """处理每个分区，分区内将所有批次合并为一个 Arrow 表后上传到 MinIO，"""
    rows = list(partition)
    if not rows:
        logger.info("Empty partition, skipping.")
        return iter([])  # 空分区返回空迭代器

    # 提取所有可能的字段
    all_columns = set()
    for row in rows:
        all_columns.update(row.asDict().keys())
    logger.info(f"Extracted columns: {all_columns}")

    # 初始化存储所有批次数据的字典
    combined_data = {col: [] for col in all_columns}

    # 分区内批次处理
    for i in range(0, len(rows), batch_size):
        batch_rows = rows[i:i + batch_size]
        logger.info(f"Processing batch {i // batch_size + 1} of {len(rows) // batch_size + 1}")
        for row in batch_rows:
            row_dict = row.asDict()
            for col in all_columns:
                combined_data[col].append(row_dict.get(col, None))

    # 将所有批次数据合并为一个 Arrow 表
    arrow_table = pa.Table.from_pydict(combined_data)
    logger.info(f"Combined data into Arrow Table with schema: {arrow_table.schema}")

    # 构造文件名
    unique_id = uuid.uuid4().hex
    object_name = f"data/{config.filename}_partition_{unique_id}.arrow"
    logger.info(f"Constructed object name: {object_name}")

    # 初始化 ArrowUploader
    arrow_uploader = ArrowUploader(config.endpoint, config.accesskey, config.secretkey, minio_bucket)
    logger.info(f"Initialized ArrowUploader with endpoint: {config.endpoint}, bucket: {minio_bucket}")

    # 上传到 MinIO
    arrow_uploader.save_to_minio(arrow_table, object_name)
    logger.info(f"Uploaded partition to MinIO: {object_name}")

    # 返回 URL
    return iter([object_name])

def notify_server_of_completion(config, object_name, total_rows):
    server_endpoint = f"{config.serverip}:{config.serverport}"
    url = urljoin(f"http://{server_endpoint}", "/api/job/completed")
    payload = {
        "objectName": object_name,
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
    将 Spark DataFrame 转换为 Arrow 表并上传到 MinIO。

    Args:
        result (DataFrame): Spark SQL 查询结果 DataFrame。
        config (Config): 配置信息对象。
        arrow_uploader (ArrowUploader): 用于上传到 MinIO 的 ArrowUploader 实例。

    Returns:
        str: 上传到 MinIO 的对象名称。
    """
    try:
        # 将结果DataFrame转换为Arrow表
        logger.info("Converting query result to Arrow Table")
        result_arrow = pa.Table.from_pandas(result.toPandas())  # Spark DataFrame 转换为 Pandas DataFrame，再转为 Arrow 表

        # 构造文件名
        unique_id = uuid.uuid4().hex
        object_name = f"{config.dataobject}_result_{unique_id}.arrow"
        logger.info(f"Constructed object name: {object_name}")

        # 上传 Arrow 表到 MinIO
        arrow_uploader.save_to_minio(result_arrow, object_name)

        # 返回对象名
        return object_name
    except Exception as e:
        logger.error(f"Error during Arrow Table conversion or upload to MinIO: {e}")
        return None

def convert_dataframe_to_arrow(df):
    """将 DataFrame 转换为 PyArrow Table。"""
    # 获取列名和数据
    columns = df.columns
    data = df.collect()

    # 创建 PyArrow Table
    arrow_table = pa.Table.from_arrays(
        [pa.array([row[col] for row in data]) for col in columns],
        names=columns
    )

    return arrow_table


def convert_partition_to_arrow(rows):
    """将每个分区的数据直接转换为 PyArrow Table。"""
    rows = list(rows)  # 将迭代器转为列表，避免重复消费

    if not rows:
        return  # 空分区直接跳过

    # 获取列名和数据
    columns = rows[0].__fields__ if hasattr(rows[0], "__fields__") else rows[0].asDict().keys()
    data = [[row[col] for col in columns] for row in rows]

    # 创建 PyArrow Table
    arrow_table = pa.Table.from_arrays(
        [pa.array([row[i] for row in data]) for i in range(len(columns))],
        names=columns
    )

    yield arrow_table

def calculate_optimal_partitions(spark, total_data_size_gb, ideal_partition_size_mb=64):
    """计算最优分区数"""
    total_cores = spark.sparkContext.defaultParallelism
    # 基于 CPU 核心数确定分区数量
    partitions_based_on_cores = total_cores * 1.5

    # 基于数据量确定分区数量
    partitions_based_on_data_size = (total_data_size_gb * 1024) / ideal_partition_size_mb

    # 取两个值的较大者
    optimal_partitions = max(partitions_based_on_cores, partitions_based_on_data_size)
    return int(optimal_partitions)


def get_data_size_gb(spark, config):
    """估算数据大小，返回单位为GB的大小"""
    # 读取数据来估算大小
    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:{config.dbtype}://{config.host}:{config.port}/{config.database}") \
        .option("dbtable", f"({config.query.strip(';')}) AS subquery") \
        .load()

    # 计算数据的大小
    total_size_bytes = df.rdd.map(lambda row: len(str(row))).sum()  # 估算每行数据的字节数
    total_size_gb = total_size_bytes / (1024 ** 3)  # 转换为 GB
    return total_size_gb

if __name__ == "__main__":
    config = parse_args()
    run_spark_job(config)
