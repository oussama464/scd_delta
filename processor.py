from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from typing import List, Dict, Any, Callable


class SCDProcessor:
    def __init__(
        self, merge_key_list: List[str], join_condition: str, hash_columns: List[str]
    ):
        self.merge_key_list = merge_key_list  # can be the primary key or a list of composite keys that uniqely identifies the record
        self.join_condition = F.expr(
            join_condition
        )  # must use src. for source_df and tgt. for target_df
        self.hash_columns = hash_columns

    def add_hash(
        self, df: DataFrame, hash_col_name: str, hashed_cols: List[str]
    ) -> DataFrame:
        return df.withColumn(
            hash_col_name,
            F.lit(
                F.sha2(
                    F.concat_ws("~", *hashed_cols),
                    256,
                )
            ),
        )

    def generate_select_expr(self, df: DataFrame):
        cols = df.columns
        select_cols = ["src.*"] + [f"tgt.{col} AS target_{col}" for col in cols]
        return select_cols

    def scd_df_builder(self, source_df: DataFrame, target_df: DataFrame) -> None:
        join_df = (
            source_df.alias("src")
            .join(target_df.alias("tgt"), self.join_condition, "left")
            .selectExpr(self.generate_select_expr(source_df))
        )
        join_df = join_df.transform(
            self.add_hash, "HashSource", self.hash_columns
        ).transform(
            self.add_hash,
            "HashTarget",
            [f"target_{col}" for col in self.hash_columns],
        )

        update_insert_df = join_df.filter("HashSource != HashTarget")

        merge_df = update_insert_df.withColumn(
            "MERGEKEY", F.concat(*self.merge_key_list)
        )
        expire_df = update_insert_df.filter(
            f"target_{self.merge_key_list[0]} IS NOT NULL"
        ).withColumn("MERGEKEY", F.lit(None))

        scd_df = merge_df.union(expire_df)
        return scd_df


scdDF = SCDProcessor(
    merge_key_list=["pk1", "pk2"],
    join_condition="src.pk1 = tgt.pk1 AND src.pk2 = tgt.pk2 AND tgt.active_status = 'Y'",
    hash_columns=["dim1", "dim2", "dim3", "dim4"],
).scd_df_builder(sourceDF, targetDF)
