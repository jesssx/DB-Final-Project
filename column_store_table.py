import csv
import io
import os
import pandas as pd
import requests
import sys

from column import Column, Compression

MAX_SIZE = 10**10 # bytes = 100 GB

def _read_csv(file_path):
  return ColumnStoreTable(file_path)

class ColumnStoreTable:
  def __init__(self, file_path):
    # columns = dictionary of column name to Column
    columns = {}

    if file_path.startswith('http://') or file_path.startswith('https://'):
      response = requests.get(file_path)
      content = response.text
      num_rows = len(content)
      file_size = len(content.encode('utf-8'))  # Calculate size from content length
    else:
      with open(file_path, 'r') as file:
        content = file.readlines()
        num_rows = len(content)
      file_size = os.path.getsize(file_path)

    # Calculate number of rows per chunk
    chunk_n_rows = None
    if file_size >= MAX_SIZE:
      row_size = file_size / num_rows
      chunk_n_rows = MAX_SIZE // row_size

    # Return Dataframe generator
    df = pd.read_csv(file_path, iterator=True, chunksize=chunk_n_rows)
    for chunk in df:
      for col in chunk.columns:
        if col not in columns:
          # print(chunk.loc[:, col])
          columns[col] = Column(col, pd.Series([chunk.loc[:, col]]))
        else:
          columns[col].add_values(pd.Series([chunk.loc[:, col]]))
    print(columns['NAME'].get_values)
    self.columns = columns

  def get_table_stats(self):
    """
    Returns dictionary of memory benchmarking statistics.
    """
    num_compressed_columns = 0
    memory_usage = sys.getsizeof(self.columns) # Only the dict.
    for col in self.columns.values():
      if col.get_compression() != Compression.NONE:
        num_compressed_columns += 1
      memory_usage += col.get_memory_usage()
    return {
        "num_columns": len(self.columns),
        "num_compressed_columns": num_compressed_columns,
        "memory_usage": memory_usage,
    }

  def compress(self, desired_compressions):
    """
    Required:
      Columns specified in desired_compressions must already be decompressed.
    Args:
      desired_compressions: dict of col_name to Compression enum, e.g. {"A": Compression.RLE, "B": Compression.BITMAP}
    Returns:
      self
    """
    for col_name, compression in desired_compressions:
      col = self.columns[col_name]

      # Check that column starts decompressed.
      assert(col.get_compression() == Compression.NONE)

      # Mutates col to be compressed.
      col.compress(compression)
    return self

  def decompress(self):
    """
    Decompresses all columns.
    """
    for col_name, col in self.columns:
      col.decompress()
    return self

  def to_row_format(self):
    # transform whole table to row format
    # call self.decompress()
    # returns a pandas dataframe generator
    self.decompress()

  def to_csv(self):
    # save a csv of the current table
    # does not return anything
    print("done")

  def filter(self, column_name, condition):
    # condition is probably a lambda function
    # filter column_name based on condition
    return self

  def merge(self, other, column_name, condition):
    # merge self with other based on column condtion
    # naive impl: uncompress column before merging
    return self

  def sort(self, column_name, ascending=True):
    # sort column [stretch]
    return self