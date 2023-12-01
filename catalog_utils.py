from delta.tables import * 
from pyspark.sql.types import * 
from pyspark.sql.functions import * 


class DataBaseNotFoundExecption(Exception):
    pass

class InitUils:
    @classmethod
    def get_current_db(cls) -> str:
        return spark.catalog.currentDatabase()

    @classmethod
    def get_db_details(cls, db_name: str):
        return spark.catalog.getDatabase(dbName=db_name)

    @classmethod
    def list_databases_in_catalog(cls):
        return [db.name for db in spark.catalog.listDatabases()]

    @classmethod
    def check_db_exists(cls, db_name: str) -> bool:
        return spark.catalog.databaseExists(dbName=db_name)

    @classmethod
    def list_tables_in_db(cls, db_name: str):
        if not cls.check_db_exists(db_name):
            raise DataBaseNotFoundExecption(
                f"db with name {db_name} does not exist in catalog"
            )
        return spark.catalog.listTables(dbName=db_name)

    @classmethod
    def set_current_db(cls, db_name: str):
        if not cls.check_db_exists(db_name):
            print(f"the database {db_name} does not exsists")
            return
        if cls.get_current_db() == db_name:
            print(f"database {db_name} is already the current database")
            return
        spark.catalog.setCurrentDatabase(db_name)
        print(f"current database set to {db_name} ")

    @classmethod
    def create_db(cls, db_name: str):
        query = f"CREATE DATABASE IF NOT EXISTS {db_name}"
        spark.sql(query)

    @classmethod
    def drop_db(cls, db_name: str):
        query = f"DROP DATABASE {db_name} CASCADE"
        spark.sql(query)
      
    @classmethod 
    def flush_delta_folder(cls,delta_path:str):
      dbutils.fs.rm(delta_path,True)

    @classmethod 
    def is_table_delta(cls,delta_path:str)->bool:
      return DeltaTable.isDeltaTable(spark,delta_path)
