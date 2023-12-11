import os
import time

from column_store_table import _read_csv, Compression

# Used for benchmarking.
def print_memory_usage(column_store_table, file):
  table_stats = column_store_table.get_table_stats()
  if (file == None):
    # print("++++++++++++++++++++++++++\n+ ColumnStoreTable stats +\n++++++++++++++++++++++++++")

    print(f"  MEMORY USAGE: {table_stats['memory_usage']} B")
    print(f"  TOTAL columns: {table_stats['num_columns']}")
    print(f"  COMPRESSED columns: {table_stats['num_compressed_columns']}\n")
  else:
    memory_usage_str = ''
    # memory_usage_str += "++++++++++++++++++++++++++\n+ ColumnStoreTable stats +\n++++++++++++++++++++++++++\n"

    memory_usage_str += f"  MEMORY USAGE: {table_stats['memory_usage']} B\n"
    memory_usage_str += f"  TOTAL columns: {table_stats['num_columns']}\n"
    memory_usage_str += f"  COMPRESSED columns: {table_stats['num_compressed_columns']}\n\n"
    return memory_usage_str

def benchmark_nyc_column_store(file="results/nyc_column.txt"):
  with open(file, "w") as output_file:
    output_file.write("Initialization: _read_csv\n")

    start_time = time.time()
    column_store_table = _read_csv('datasets/files/central_park_weather.csv')
    end_time = time.time()

    execution_time = (end_time - start_time) * 1000
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")
    output_file.write(print_memory_usage(column_store_table, file))


    # RLE compress first 2 columns.
    output_file.write("\nCompression: station, name col compress RLE\n")
    start_time = time.time()
    column_store_table.compress({
        "STATION": Compression.RLE,  # Only has 1 value.
        "NAME": Compression.RLE,  # Only has 1 value.
    })
    end_time = time.time()

    execution_time = (end_time - start_time) * 1000
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")
    output_file.write(print_memory_usage(column_store_table, file))

    output_file.write("\nDecompression\n")
    start_time = time.time()
    column_store_table.decompress()
    end_time = time.time()

    execution_time = (end_time - start_time) * 1000
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")
    output_file.write(print_memory_usage(column_store_table, file))


    # Bitmap compress first 2 columns.
    output_file.write("\nCompression: station, name col compress BITMAP\n")
    start_time = time.time()
    column_store_table.compress({
        "STATION": Compression.BITMAP,  # Only has 1 value.
        "NAME": Compression.BITMAP,  # Only has 1 value.
    })
    end_time = time.time()

    execution_time = (end_time - start_time) * 1000
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")
    output_file.write(print_memory_usage(column_store_table, file))

    output_file.write("\nDecompression\n")
    start_time = time.time()
    column_store_table.decompress()
    end_time = time.time()

    execution_time = (end_time - start_time) * 1000
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")
    output_file.write(print_memory_usage(column_store_table, file))


    # Sort by AWND.
    output_file.write("\nSort by AWND\n") 
    start_time = time.time()
    column_store_table.sort("AWND")
    end_time = time.time()

    execution_time = (end_time - start_time) * 1000
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")
    output_file.write(print_memory_usage(column_store_table, file))


    # Filter.
    output_file.write("\nFilter: SNWD != '0.0'\n")
    start_time = time.time()
    column_store_table.filter("SNWD", lambda x: x != "0.0")
    end_time = time.time()

    execution_time = (end_time - start_time) * 1000
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")
    output_file.write(print_memory_usage(column_store_table, file))
    

    # Merge.
    output_file.write("\nMerge: self merge on DATE\n")
    other = _read_csv('datasets/files/central_park_weather.csv')
    start_time = time.time()
    column_store_table.merge(other, "DATE")

    # TODO: Debug. 
    # merged_table = column_store_table.merge(other, "DATE")
    # print(merged_table.get_table_stats())
    # output_file.write("merged_table:\n")
    # output_file.write(print_memory_usage(merged_table, file))

    end_time = time.time()

    execution_time = (end_time - start_time) * 1000
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")


    # To csv.
    output_file.write("\nto_csv\n")
    to_csv_file = "results/nyc_column.csv"
    start_time = time.time()
    column_store_table.to_csv(to_csv_file)
    end_time = time.time()
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")
    os.remove(to_csv_file) # Remove the saved file.

def benchmark_nyc_column_store_optimal(file="results/nyc_column_optimal.txt"):
  with open(file, "w") as output_file:
    column_store_table = _read_csv('datasets/files/central_park_weather.csv')

    start_time = time.time()
    column_store_table.sort("SNOW")
    column_store_table.compress({
        "STATION": Compression.BITMAP,  # Only has 1 value.
        "NAME": Compression.BITMAP,  # Only has 1 value.
        "AWND": Compression.RLE,
    })
    end_time = time.time()

    execution_time = (end_time - start_time) * 1000
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")

    output_file.write(print_memory_usage(column_store_table, file))

def main():
  benchmark_nyc_column_store()
  benchmark_nyc_column_store_optimal()

if __name__ == "__main__":
  main()