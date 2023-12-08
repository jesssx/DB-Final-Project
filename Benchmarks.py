from column_store_table import _read_csv, Compression

# Used for benchmarking.
def print_memory_usage(column_store_table):
  table_stats = column_store_table.get_table_stats()
  print("ColumnStoreTable stats")

  print(f"  MEMORY USAGE: {table_stats['memory_usage']} B")
  print(f"  TOTAL columns: {table_stats['num_columns']}")
  print(f"  COMPRESSED columns: {table_stats['num_compressed_columns']}")

def benchmark_nyc_taxi_data():
  print("INITIALIZATION\n")
  # TODO: Can't read the link. Works in colab.
  column_store_table = _read_csv('https://raw.githubusercontent.com/toddwschneider/nyc-taxi-data/master/data/central_park_weather.csv')
  print_memory_usage(column_store_table)

  # RLE compress first 2 columns. 
  print("\n\nCOMPRESSION\n")
  column_store_table.compress({
      "STATION": Compression.RLE, # Only has 1 value.
      "NAME": Compression.RLE,  # Only has 1 value.
      })
  print_memory_usage(column_store_table)

  print("\n\nDECOMPRESSION\n")
  column_store_table.decompress()
  print_memory_usage(column_store_table)

def main():
  benchmark_nyc_taxi_data()

if __name__ == "__main__":
  main()