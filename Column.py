# New column store design.
from enum import Enum
import pandas as pd
import io
import os
import sys

class Compression(Enum):
    NONE = 0
    RLE = 1
    BITMAP = 2
    DELTA = 3

class Column:
  def __init__(self, name, values):
    self.name = name
    self.values = values # list of pd.Series

    # Column must be uncompressed when first initialized.
    self.compression = Compression.NONE

  def get_name(self):
    return self.name

  def get_values(self):
    return self.values

  def get_compression(self):
    return self.compression

  def get_memory_usage(self):
    memory_usage = sys.getsizeof(self.values)
    for chunk in self.values:
      memory_usage += chunk.memory_usage() # TODO: Should index be T or F?
    return memory_usage

  def compress(self, compression):
    self.compression = compression
    match self.compression:
      case Compression.RLE:
        self.compress_RLE()
      case Compression.BITMAP:
        self.compress_BITMAP()
      case Compression.DELTA:
        self.compress_DELTA()
    # Else, Compression.NONE, do nothing.
    return self

  def compress_RLE(self):
    """
    Run length encodes the column and mutates self.values to hold new pd.Series
    instances.
    Result:
      pd.Series objects in self.values have run_length and value fields.
    """
    self.compression = Compression.RLE
    # Perform run-length encoding
    for i, chunk in enumerate(self.values):
      values = chunk.ne(chunk.shift()).cumsum()
      run_lengths = chunk.groupby(values).size()

      # Create a new Series with run-length encoded values and run lengths
      compressed_chunk = pd.Series(
          {'value': chunk.groupby(values).first().values, 'run_length': run_lengths.values},
      )

      # Mutate self.values.
      self.values[i] = compressed_chunk

    return self

  def compress_DELTA(self):
    return self

  def compress_BITMAP(self):
    return self

  # TODO: Sanity check and test equality.
  def decompress(self):
    match self.compression:
      case Compression.RLE:
        self.decompress_RLE()
      case Compression.BITMAP:
        self.decompress_BITMAP()
      case Compression.DELTA:
        self.decompress_DELTA()
    # Else, Compression.NONE, do nothing.
    return self

    def decompress_RLE(self):
      """
      Decompresses the column back to its original format.
      """
      if self.compression != Compression.RLE:
        raise ValueError("Column is not compressed with RLE.")

      # Perform run-length decoding
      for i, compressed_chunk in enumerate(self.values):
        original_values = []
        for value, run_length in zip(compressed_chunk['value'], compressed_chunk['run_length']):
            original_values.extend([value] * run_length)

        # Create a new Series with the original values
        original_chunk = pd.Series(original_values)

        # Mutate self.values.
        self.values[i] = original_chunk

      # Reset compression type
      self.compression = Compression.NONE

      return self

  def decompress_DELTA(self):
    return self

  def decompress_BITMAP(self):
    return self

  def add_values(self, new_val):
    self.values.append(new_val)

  # add other compression methods like bitmap and delta