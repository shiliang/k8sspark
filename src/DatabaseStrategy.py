from abc import ABC, abstractmethod


class DatabaseStrategy(ABC):
    """抽象基类，定义数据库策略的接口。"""

    @abstractmethod
    def get_jdbc_url(self) -> str:
        """返回JDBC URL。"""
        pass

    @abstractmethod
    def get_driver(self) -> str:
        """返回JDBC驱动类名。"""
        pass


class KingbaseStrategy(DatabaseStrategy):
    """Kingbase数据库策略类。"""

    def __init__(self, host: str, port: int, database: str, username: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

    def get_jdbc_url(self) -> str:
        return f"jdbc:kingbase8://{self.host}:{self.port}/{self.database}?user={self.username}&password={self.password}"

    def get_driver(self) -> str:
        return "com.kingbase8.Driver"


class MySQLStrategy(DatabaseStrategy):
    """MySQL数据库策略类。"""

    def __init__(self, host: str, port: int, database: str, username: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

    def get_jdbc_url(self) -> str:
        return f"jdbc:mysql://{self.host}:{self.port}/{self.database}?user={self.username}&password={self.password}"

    def get_driver(self) -> str:
        return "com.mysql.cj.jdbc.Driver"


class DatabaseFactory:
    """工厂类，根据数据库类型生成对应的策略实例。"""

    @staticmethod
    def get_strategy(db_type: str, host: str, port: int, database: str, username: str, password: str) -> DatabaseStrategy:
        if db_type == "1":
            return MySQLStrategy(host, port, database, username, password)
        elif db_type == "2":
            return KingbaseStrategy(host, port, database, username, password)
        else:
            raise ValueError("Unknown database type")
