import time
import pandas as pd

def print_memory_usage(df):
  memory_usage_per_col = df.memory_usage()
  return f"  MEMORY USAGE: {memory_usage_per_col.sum()} B\n"

def benchmark_nyc_pandas(file="results/nyc_pandas.txt"):
  with open(file, "w") as output_file:
    output_file.write("Initialization: _read_csv\n")

    start_time = time.time()
    df = pd.read_csv('datasets/files/central_park_weather.csv')
    end_time = time.time()

    execution_time = (end_time - start_time) * 1000
    output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")
    output_file.write(print_memory_usage(df))

    # TODO: Make the following use pandas funcs.
    # # Sort by AWND.
    # output_file.write("\nSort by AWND\n") 
    # start_time = time.time()
    # df.sort("AWND")
    # end_time = time.time()

    # execution_time = (end_time - start_time) * 1000
    # output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")

    # # Filter.
    # # TODO: Uncomment when ready.
    # # output_file.write("\nFilter: SNWD != '0.0'\n")
    # # start_time = time.time()
    # # df.filter("SNWD", lambda x: x != "0.0")
    # # end_time = time.time()

    # execution_time = (end_time - start_time) * 1000
    # output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")

    # # Merge.
    # output_file.write("\nMerge: self merge on DATE\n")
    # other = _read_csv('datasets/files/central_park_weather.csv')
    # start_time = time.time()
    # df.merge(other, "DATE")

    # # TODO: Debug. 
    # # merged_table = df.merge(other, "DATE")
    # # print(merged_table.get_table_stats())
    # # output_file.write("merged_table:\n")
    # # output_file.write(print_memory_usage(merged_table, file))

    # end_time = time.time()

    # execution_time = (end_time - start_time) * 1000
    # output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")


    # # To csv.
    # output_file.write("\nto_csv\n")
    # start_time = time.time()
    # df.to_csv("results/nyc_column.csv")
    # end_time = time.time()
    # output_file.write(f"  EXECUTION TIME: {execution_time} ms\n")

def main():
  benchmark_nyc_pandas()

if __name__ == "__main__":
  main()