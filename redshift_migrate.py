import boto3
from dataclasses import dataclass
from enum import Enum

TABLES = [
    {
        "table_name": "sales_v2",
        "query": """
        CREATE EXTERNAL TABLE {external_schema_name}.sales_v2 (
            salesid INTEGER,
            listid INTEGER,
            sellerid INTEGER,
            buyerid INTEGER,
            eventid INTEGER,
            dateid SMALLINT,
            qtysold SMALLINT,
            pricepaid DECIMAL(8,2),
            commission DECIMAL(8,2),
            saletime TIMESTAMP
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '|'
        LOCATION '{s3_location}'
        TABLE PROPERTIES ('numRows'='172000');
        """
        # Add more table creation queries as needed
    }
]


class QueryStates(Enum):
    FAILED = "FAILED"
    FINISHED = "FINISHED"
    ABORTED = "ABORTED"


@dataclass
class Params:
    database_name: str
    external_schema_name: str
    data_catalog_database: str
    iam_role_arn: str
    s3_location: str
    work_group_name: str


class InitRedshiftTablesAndScheam:
    def __init__(self, params: Params, redshift_data) -> None:
        self.params = params
        self.redshift_data = redshift_data

    def wait_and_check_response(self, query_id: str) -> QueryStates:
        while True:
            status_response = self.redshift_data.describe_statement(Id=query_id)
            status = status_response["Status"]
            if status in ["FINISHED", "FAILED", "ABORTED"]:
                break
        return QueryStates(status)

    def create_redshift_schema(self) -> str:
        create_schema_query = f"""
                CREATE EXTERNAL SCHEMA {self.params.external_schema_name}
                FROM DATA CATALOG 
                DATABASE '{self.params.data_catalog_database}'
                IAM_ROLE '{self.params.iam_role_arn}'
                CREATE EXTERNAL DATABASE IF NOT EXISTS
            """

        response = self.redshift_data.execute_statement(
            Database=self.params.database_name,
            Sql=create_schema_query,
            WorkgroupName=self.params.work_group_name,
        )
        query_id = response["Id"]
        return query_id

    def table_queries_factory(self, table_queries):
        new_table_queries = []
        for query_dict in table_queries:
            query = query_dict["query"].format(
                external_schema_name=self.params.external_schema_name,
                s3_location=self.params.s3_location,
            )
            new_table_queries.append(
                {"table_name": query_dict["table_name"], "query": query}
            )
        return new_table_queries

    def create_tables(self, table_queries):
        table_query_map = self.table_queries_factory(table_queries)
        for table in table_query_map:
            response = redshift_data.execute_statement(
                Database=self.params.database_name,
                Sql=table["query"],
                WorkgroupName=self.params.work_group_name,
            )
            table_query_id = response["Id"]
            table_status = self.wait_and_check_response(table_query_id)
            if table_status != QueryStates.FINISHED:
                print(f"Table {table['table_name']} creation failed")
                continue
            else:
                print(f"Table {table['table_name']} creation suceeded")


if __name__ == "__main__":
    params = Params(
        database_name="dev",
        external_schema_name="ouss_schema",
        data_catalog_database="ouss_db",
        iam_role_arn="",
        s3_location="",
        work_group_name="workgroup",
    )
    redshift_data = boto3.client("redshift-data")
    init_table_schema = InitRedshiftTablesAndScheam(params, redshift_data)
    # schema_query_id = init_table_schema.create_redshift_schema()
    # schema_status = init_table_schema.wait_and_check_response(schema_query_id)
    # if schema_status != QueryStates.FINISHED:
    #     print(f"Schema {params.external_schema_name} creation failed ")
    # else:
    #     print(f"Schema {params.external_schema_name} creation succeeded")
    # print("Proceeding to table creation")

    init_table_schema.create_tables(TABLES)