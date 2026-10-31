from dataclasses import dataclass
import os
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    input_path: Path
    data_dir: Path
    log_dir: Path
    spark_master: str
    warehouse_enabled: bool
    pg_host: str
    pg_port: int
    pg_database: str
    pg_user: str
    pg_password: str

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        return cls(
            input_path=Path(os.getenv("INPUT_PATH", "data/input/online_retail.xlsx")),
            data_dir=Path(os.getenv("DATA_DIR", "data")),
            log_dir=Path(os.getenv("LOG_DIR", "logs")),
            spark_master=os.getenv("SPARK_MASTER", "local[*]"),
            warehouse_enabled=os.getenv("WAREHOUSE_ENABLED", "false").lower() == "true",
            pg_host=os.getenv("PGHOST", "localhost"),
            pg_port=int(os.getenv("PGPORT", "5432")),
            pg_database=os.getenv("PGDATABASE", "retail"),
            pg_user=os.getenv("PGUSER", "retail_user"),
            pg_password=os.getenv("PGPASSWORD", ""),
        )

    def pg_connect_args(self) -> dict:
        """Return libpq connection options without hand-building a password string."""
        return {
            "host": self.pg_host,
            "port": self.pg_port,
            "dbname": self.pg_database,
            "user": self.pg_user,
            "password": self.pg_password,
        }
