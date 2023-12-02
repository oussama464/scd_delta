from delta.tables import DeltaTable 

def post_merge_recap(table_path:str):
  cols = ["operation","executionTimeMs","numOutputRows","numSourceRows","numTargetRowsInserted","numTargetRowsUpdated","numTargetRowsDeleted"]
  my_table = DeltaTable.forPath(spark,table_path)
  lastOperationdf = my_table.history(limit=1) # get the last operation only 
  explode_df = lastOperationdf.select(lastOperationdf.operation,explode(lastOperationdf.operationMetrics))
  explode_df_select = explode_df.select(explode_df.operation,explode_df.key,explode_df.value.cast("int"))
  pivot_df = explode_df_select.groupBy("operation").pivot("key").sum("value")
  info_df = pivot_df.select(*cols)
  return info_df 