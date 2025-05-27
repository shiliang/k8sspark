import logging
import os
from datetime import timedelta

from minio import Minio, S3Error
import pyarrow.ipc as ipc
import io
import pyarrow as pa
import uuid

# 初始化日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ArrowUploader:

    def __init__(self, endpoint, access_key, secret_key, bucket_name):
        logger.info(f"Initializing ArrowUploader with endpoint: {endpoint}, bucket_name: {bucket_name}")
        self.minio_client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        self.bucket_name = bucket_name

    def save_to_minio(self, arrow_table, object_name):
        byte_stream = io.BytesIO()

        # 使用 PyArrow IPC 将数据写入 ByteArrayOutputStream
        with ipc.new_file(byte_stream, arrow_table.schema) as writer:
            writer.write_table(arrow_table)

        byte_stream.seek(0)  # 重置流

        # 上传文件到 MinIO，打印内容验证流数据
        logger.debug(f"Stream data (first 256 bytes): {byte_stream.getvalue()[:256]}")

        # 上传到 MinIO
        self.minio_client.put_object(
            bucket_name=self.bucket_name,
            object_name=object_name,
            data=byte_stream,
            length=len(byte_stream.getvalue()),
            content_type='application/octet-stream'
        )
        logger.info(f"Uploaded {object_name} successfully to MinIO.")

    def generate_presigned_url(self, object_name, expires=3600):
        """生成预签名 URL"""
        try:
            # 确保 `expires` 是一个有效的 timedelta 对象
            if not isinstance(expires, timedelta):
                if isinstance(expires, int):
                    expires = timedelta(seconds=expires)  # 如果是 int，转换为 timedelta
                else:
                    raise ValueError("The 'expires' parameter must be an integer (seconds) or a timedelta object.")

            # 调用 MinIO 客户端生成预签名 URL
            presigned_url = self.minio_client.presigned_get_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                expires=expires,
            )
            logger.info(f"Generated presigned URL for {object_name}: {presigned_url}")
            return presigned_url
        except ValueError as ve:
            logger.error(f"Invalid 'expires' value: {ve}")
        except S3Error as e:
            logger.error(f"Error generating presigned URL for {object_name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error generating presigned URL for {object_name}: {e}")
        return None

    def read_from_minio(self, spark, bucket_name, object_name, schema=None):
        try:
            # 使用 self.minio_client.get_object 直接获取文件内容
            obj = self.minio_client.get_object(bucket_name, object_name)
            logger.info(f"Downloaded {object_name} successfully from MinIO.")
        except S3Error as e:
            logger.error(f"Error downloading {object_name} from MinIO: {e}")
            raise  # 如果下载失败，抛出异常

        # 将 MinIO 对象加载到内存中
        file_bytes = io.BytesIO(obj.read())  # 将返回的对象内容读入内存
        logger.info(f"Loaded {object_name} into memory.")

        # 读取 Arrow 文件的函数
        def mapper(iterator):
            with pa.ipc.open_file(file_bytes) as f:  # 直接使用内存中的字节流
                logger.info(f"Arrow file {object_name} opened successfully.")
                for batch in iterator:
                    for i in batch['id']:
                        # 检查并处理不同类型
                        if isinstance(i, pa.lib.Int8Array):
                            id_value = i.as_py()  # 转换为 Python 原生类型
                        elif isinstance(i, pa.lib.Int16Array):
                            id_value = i.as_py()
                        elif isinstance(i, pa.lib.Int32Array):
                            id_value = i.as_py()
                        elif isinstance(i, pa.lib.Int64Array):
                            id_value = i.as_py()
                        elif isinstance(i, pa.lib.UInt8Array):
                            id_value = i.as_py()
                        elif isinstance(i, pa.lib.UInt16Array):
                            id_value = i.as_py()
                        elif isinstance(i, pa.lib.UInt32Array):
                            id_value = i.as_py()
                        elif isinstance(i, pa.lib.UInt64Array):
                            id_value = i.as_py()
                        elif isinstance(i, pa.FloatArray):
                            id_value = i.as_py()
                        elif isinstance(i, pa.lib.StringArray):
                            id_value = i.as_py()  # 字符串
                        elif isinstance(i, pa.lib.BinaryArray):
                            id_value = i.as_py()  # 二进制数据
                        elif isinstance(i, pa.lib.TimestampArray):
                            id_value = i.as_py()  # 时间戳
                        elif isinstance(i, pa.lib.Date32Array) or isinstance(i, pa.lib.Date64Array):
                            id_value = i.as_py()  # 日期
                        elif isinstance(i, pa.lib.Time32Array) or isinstance(i, pa.lib.Time64Array):
                            id_value = i.as_py()  # 时间
                        elif isinstance(i, pa.lib.ListArray):
                            id_value = i.to_pandas()  # 列表类型
                        elif isinstance(i, pa.lib.LargeListArray):
                            id_value = i.to_pandas()  # 大列表类型
                        elif isinstance(i, pa.lib.StructArray):
                            id_value = i.to_pandas()  # 结构体类型
                        elif isinstance(i, (int, float, str, bool)):  # 直接是原生 Python 类型
                            id_value = i
                        else:
                            logger.warning(f"Unexpected data type in batch['id']: {type(i)}")
                            continue  # 跳过未知数据类型

                        logger.debug(f"Processing batch with id: {id_value}")
                        yield f.get_batch(id_value).to_pandas()

        # 获取 Arrow 文件的总批次数
        try:
            tmp_reader = pa.ipc.open_file(file_bytes)
            num_batches = tmp_reader.num_record_batches
            logger.info(f"Arrow file contains {num_batches} batches.")
        except Exception as e:
            logger.error(f"Failed to open Arrow file: {e}")
            raise  # 如果文件读取失败，抛出异常

        # 如果没有传递 schema，自动推导 schema
        if schema is None:
            try:
                tmp_row = tmp_reader.get_batch(0)[:1]
                schema = spark.createDataFrame(tmp_row.to_pandas()).schema
                logger.info("Schema inferred from the first batch.")
            except Exception as e:
                logger.error(f"Failed to infer schema: {e}")
                raise  # 如果推导 schema 失败，抛出异常

        # 使用 mapInPandas 来转换每个批次
        logger.info(f"Starting to process batches with inferred schema: {schema}")
        result_df = spark.range(num_batches).mapInPandas(mapper, schema)
        logger.info(f"Finished processing Arrow file and returning DataFrame.")

        return result_df

    def save_hash_column_to_csv(self, spark_df, column_name, object_name=None):
        """
        将DataFrame中的指定列保存为CSV文件到MinIO
        
        Args:
            spark_df: Spark DataFrame
            column_name: 要保存的列名
            object_name: 保存的文件名，如果不指定则自动生成
            
        Returns:
            str: 保存的文件名
        """
        import uuid
        import io
        
        # 如果没有指定文件名，生成一个
        if object_name is None:
            object_name = f"data/hash_values_{uuid.uuid4().hex}.csv"
        
        # 只选择指定的列
        hash_df = spark_df.select(column_name)
        
        # 将DataFrame转换为CSV格式的字符串
        csv_data = hash_df.rdd.map(lambda row: str(row[0])).collect()
        csv_content = '\n'.join(csv_data)
        
        # 创建字节流对象
        byte_stream = io.BytesIO(csv_content.encode('utf-8'))
        
        # 上传到MinIO
        self.minio_client.put_object(
            bucket_name=self.bucket_name,
            object_name=object_name,
            data=byte_stream,
            length=len(csv_content.encode('utf-8')),
            content_type='text/csv'
        )
        
        logger.info(f"Hash values saved to MinIO as {object_name}")
        return object_name

    def read_arrow_files_as_partitions_optimized(self, spark, config):
        """优化版本：使用mapPartitions直接处理Arrow文件"""
        try:
            import pyarrow as pa
            import pyarrow.ipc as ipc
            import pandas as pd
            import io
            
            file_paths = config.inobjects
            logger.info(f"Reading {len(file_paths)} Arrow files using optimized partition method")
            
            # 创建一个包含文件路径的RDD，每个文件路径一个分区
            file_rdd = spark.sparkContext.parallelize(file_paths, len(file_paths))
            
            # 提取MinIO连接参数，避免序列化self对象
            endpoint = config.endpoint
            access_key = config.accesskey
            secret_key = config.secretkey
            bucket = config.bucket
            
            def process_arrow_file_partition(iterator):
                """处理单个分区中的Arrow文件"""
                # 在executor中重新创建MinIO客户端
                from minio import Minio
                import pyarrow.ipc as ipc
                import io
                
                minio_client = Minio(
                    endpoint=endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=False
                )
                
                for file_path in iterator:
                    try:
                        # 从MinIO下载文件
                        obj = minio_client.get_object(bucket, file_path)
                        file_bytes = io.BytesIO(obj.read())
                        
                        # 读取Arrow文件
                        with ipc.open_file(file_bytes) as reader:
                            for i in range(reader.num_record_batches):
                                batch = reader.get_batch(i)
                                pandas_df = batch.to_pandas()
                                
                                # 逐行yield数据
                                for _, row in pandas_df.iterrows():
                                    yield tuple(row)
                        
                        logger.info(f"Processed Arrow file {file_path} in partition")
                        
                    except Exception as e:
                        logger.error(f"Error processing Arrow file {file_path}: {e}")
                        continue
            
            # 获取schema（从第一个文件）使用self.minio_client
            first_obj = self.minio_client.get_object(config.bucket, file_paths[0])
            first_bytes = io.BytesIO(first_obj.read())
            
            with ipc.open_file(first_bytes) as reader:
                first_batch = reader.get_batch(0)
                first_df = first_batch.to_pandas()
                spark_schema = spark.createDataFrame(first_df).schema
            
            logger.info(f"Inferred schema from first Arrow file: {spark_schema}")
            
            # 应用mapPartitions处理
            processed_rdd = file_rdd.mapPartitions(process_arrow_file_partition)
            
            # 转换为DataFrame
            result_df = spark.createDataFrame(processed_rdd, spark_schema)
            
            logger.info(f"Created DataFrame with {result_df.rdd.getNumPartitions()} partitions from Arrow files")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error in optimized Arrow file reading: {e}")
            raise
