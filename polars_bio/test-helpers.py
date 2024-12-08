# def overlap_polars(df1: DataFrame, df2: DataFrame):
#     result = overlap_internal( df1.to_arrow().to_reader(),  df2.to_arrow().to_reader())
#     return  result.to_polars()
#
# def overlap_polars_datafusion(df1: DataFrame, df2: DataFrame) -> datafusion.dataframe.DataFrame:
#     result = overlap_internal( df1.to_arrow().to_reader(),  df2.to_arrow().to_reader())
#     return  result
#
# def overlap_pandas(df1: DataFrame, df2: DataFrame):
#     result = overlap_internal( df1.to_arrow().to_reader(),  df2.to_arrow().to_reader())
#     return  result.to_pandas()
#
# def overlap_arrow(df1: RecordBatchReader, df2: RecordBatchReader):
#     result = overlap_internal(df1, df2)
#     return  result.to_arrow_table()
#
# def overlap_arrow_datafusion(df1: RecordBatchReader, df2: RecordBatchReader):
#     result = overlap_internal(df1, df2)
#     return  result

