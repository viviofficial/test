def MyTransform (glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col
    from pyspark.sql.functions import expr
    from delta.tables import DeltaTable
    args = getResolvedOptions(sys.argv, ['JOB_NAME','s3_bucket'])
    
    cdc_df=OracleSQL_node1679049806888
    cdc_df=cdc_df.toDF()
    
    # now read the full load (latest data) as delta table
    delta_df = DeltaTable.forPath(spark, "s3://"+ args['s3_bucket']+"/delta/")
    delta_df.toDF()
    
    # UPSERT process if matches on the condition the update else insert
    # if there is no keyword then create a data set with Insert, Update and Delete flag and do it separately.
    # for delete it has to run in loop with delete condition, this script do not handle deletes.
    
    final_df = delta_df.alias("prev_df").merge( \
    source = cdc_df.alias("append_df"), \
    #matching on primarykey
    condition = expr("prev_df.resource_id = append_df.resource_id"))\
    .whenMatchedUpdate(set= {
    "prev_df.employee_id"         : col("append_df.employee_id"),
    "prev_df.middle_name"           : col("append_df.middle_name")})\
    .whenNotMatchedInsert(values =
    #inserting a new row to Delta table
    {   
    "prev_df.resource_id"           : col("append_df.resource_id"), 
    "prev_df.employee_id"         : col("append_df.employee_id")
    })\
    .execute()