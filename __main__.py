import argparse
import logging
import uuid

import requests
import pyarrow as pa
from pyspark.sql import SparkSession
from urllib.parse import urljoin

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
        self.inobjects = kwargs.get('inobjects', []) # where xxx in字段对应的数据文件
        self.query = kwargs.get('query', '')
        self.endpoint = kwargs.get('endpoint', '')
        self.incolumns = kwargs.get('incolumns', []) # where xxx in字段
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
    parser.add_argument("--inobjects", type=str, nargs='+', help="List of in params object located in OSS")
    parser.add_argument("--incolumns", type=str, nargs='+', help="List of columns for IN clause")
    parser.add_argument("--endpoint", type=str, required=True, help="endpoint in OSS")
    parser.add_argument("--columns", type=str, nargs='+', help="List of columns to SELECT")
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

    arrow_uploader = ArrowUploader(config.endpoint, config.accesskey, config.secretkey, config.bucket)

    try:
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
    except Exception as e:
        logger.error(f"Error writing DataFrame to MinIO: {e}")
    finally:
        spark.stop()

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
        object_name = f"result_{unique_id}.arrow"
        logger.info(f"Constructed object name: {object_name}")

        # 上传 Arrow 表到 MinIO
        arrow_uploader.save_to_minio(result_arrow, object_name)

        # 返回对象名
        return object_name
    except Exception as e:
        logger.error(f"Error during Arrow Table conversion or upload to MinIO: {e}")
        return None

def read_dataframe_from_minio(arrow_uploader, spark, bucket, object):
    return arrow_uploader.read_from_minio(spark, bucket, object)

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
