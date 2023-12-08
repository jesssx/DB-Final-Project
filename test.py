import pandas as pd

from column import Column
from column_store_table import ColumnStoreTable

def test_RLE_single_column():
    """
    Tests RLE compression on a Column instance. With memory printouts.
    """
    data = [0, 0, 1, 1, 1, 1, 1, 0]
    col_series = pd.Series(data, name='Column1')
    col = Column("A", [col_series])

    # initialization
    print("INITIALIZATION\n")
    print(col_series.memory_usage()) # 192 B
    print("col get_memory_usage:\n", col.get_memory_usage()) # 256 B
    print(col.get_values())

    # compression
    print("\n\nCOMPRESSION\n")
    col.compress_RLE()
    print(col.get_values()[0].value)
    print(col.get_values()[0].run_length)
    print("col get_memory_usage:\n", col.get_memory_usage()) # 204 B

    # decompression
    print("\n\nDECOMPRESSION\n")
    col.decompress_RLE()
    print(col.get_values())
    print("col get_memory_usage:\n", col.get_memory_usage())

def main():
    test_RLE_single_column()

if __name__ == "__main__":
    main()