import pandas as pd

from column import Column
from column_store_table import ColumnStoreTable


def test_RLE_single_column():
    """
    Tests RLE compression on a Column instance. With memory printouts.
    """
    data = [0, 0, 1, 1, 1, 1, 1, 0]
    col_series = pd.Series(data, name="Column1")
    col = Column("A", [col_series])

    # initialization
    print("INITIALIZATION\n")
    print(col_series.memory_usage())  # 192 B
    print("col get_memory_usage:\n", col.get_memory_usage())  # 256 B
    print(col.get_values())

    # compression
    print("\n\nCOMPRESSION\n")
    col.compress_RLE()
    print(col.get_values()[0].value)
    print(col.get_values()[0].run_length)
    print("col get_memory_usage:\n", col.get_memory_usage())  # 204 B

    # decompression
    print("\n\nDECOMPRESSION\n")
    col.decompress_RLE()
    print(col.get_values())
    print("col get_memory_usage:\n", col.get_memory_usage())


def test_merge():
    ids = [1, 2, 3, 4, 5]
    names = ["A", "B", "C", "D", "E"]
    names2 = ["A", "C", "A", "D", "E"]
    hobbies = ["cooking", "baking", "swimming", "running", "reading"]

    col1 = Column("ids", pd.Series(ids))
    col2 = Column("names", pd.Series(names))
    col3 = Column("names", pd.Series(names2))
    col4 = Column("hobbies", pd.Series(hobbies))

    id_table = ColumnStoreTable(None)
    id_table.add_column(col1.name, col1)
    id_table.add_column(col2.name, col2)
    hobby_table = ColumnStoreTable(None)
    hobby_table.add_column(col3.name, col3)
    hobby_table.add_column(col4.name, col4)

    merged_table = id_table.merge(hobby_table, "names")


def test_sort():
    ids = [3, 2, 1, 4, 5]
    names = ["C", "B", "A", "D", "E"]

    col1 = Column("ids", pd.Series(ids))
    col2 = Column("names", pd.Series(names))

    id_table = ColumnStoreTable(None)
    id_table.add_column(col1.name, col1)
    id_table.add_column(col2.name, col2)

    print("BEFORE SORTING")
    columns = id_table.get_columns()
    expected = {"ids": [3, 2, 1, 4, 5], "names": ["C", "B", "A", "D", "E"]}

    for name, col in columns.items():
        result = list(col.get_values())
        for i in range(len(expected[name])):
            assert expected[name][i] == result[i]

    print("SORTED BY ID (ASCENDING)")
    id_table.sort("ids")
    columns = id_table.get_columns()

    expected = {"ids": [1, 2, 3, 4, 5], "names": ["A", "B", "C", "D", "E"]}
    for name, col in columns.items():
        result = list(col.get_values())
        for i in range(len(expected[name])):
            assert expected[name][i] == result[i]

    print("SORTED BY ID (DESCENDING)")
    id_table.sort("ids", False)
    columns = id_table.get_columns()

    expected = {"ids": [5, 4, 3, 2, 1], "names": ["E", "D", "C", "B", "A"]}
    for name, col in columns.items():
        result = list(col.get_values())
        for i in range(len(expected[name])):
            assert expected[name][i] == result[i]

    print("ALL SORTED CORRECTLY")


def test_bitmap_single_column():
    data = ["M", "F", "F", "M", "M"]
    col_series = pd.Series(data, name="Column1")
    col = Column("Gender", col_series)

    # initialization
    print("INITIALIZATION\n")
    print("col get_memory_usage:\n", col.get_memory_usage())  # 172 B
    print(col.get_values())

    # compression
    print("\n\nCOMPRESSION\n")
    col.compress_BITMAP()
    print(col.get_values())
    print("col get_memory_usage:\n", col.get_memory_usage())  # 32 B

    # decompression
    print("\n\nDECOMPRESSION\n")
    col.decompress_BITMAP()
    print(col.get_values())
    print("col get_memory_usage:\n", col.get_memory_usage())


def main():
    # test_RLE_single_column()
    # test_merge()
    # test_sort()
    test_bitmap_single_column()


if __name__ == "__main__":
    main()
