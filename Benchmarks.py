from WrapperFunctions import _read_csv

# Used for benchmarking.
def print_memory_usage(column_store_table):
  table_stats = column_store_table.get_table_stats()
  print("ColumnStoreTable stats")

  print(f"  MEMORY USAGE: {table_stats['memory_usage']} B")
  print(f"  TOTAL columns: {table_stats['num_columns']}")
  print(f"  COMPRESSED columns: {table_stats['num_compressed_columns']}")

column_store_table = _read_csv('https://raw.githubusercontent.com/toddwschneider/nyc-taxi-data/master/data/central_park_weather.csv')
print_memory_usage(column_store_table)