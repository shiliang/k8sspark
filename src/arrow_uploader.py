import logging
from datetime import timedelta

from minio import Minio, S3Error
import pyarrow.ipc as ipc
import io

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