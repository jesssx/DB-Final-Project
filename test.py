import pandas as pd
from column import Column, Compression
from column_store_table import ColumnStoreTable, _read_csv


def test_RLE_single_column():
    """
    Tests RLE compression on a Column instance. With memory printouts.
    """
    # data = [0, 0, 1, 1, 1, 1, 1, 0]
    data = ["M", "F", "F", "M", "M"]
    col_series = pd.Series(data, name="Column1")
    col = Column("A", col_series)

    # initialization
    print("INITIALIZATION\n")
    print(col_series.memory_usage())  # 192 B
    print("col get_memory_usage:\n", col.get_memory_usage())  # 256 B
    print(col.get_values())

    # compression
    print("\n\nCOMPRESSION\n")
    col.compress_RLE()
    print(col.get_values().value)
    print(col.get_values().run_length)
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
    print(merged_table.get_table_stats())


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


def test_read_csv_compressions_people():
    people = _read_csv("datasets/files/people/people-2000000.csv")
    people.print_column_stats()
    print(people.get_table_stats())
    people.compress({"Company": Compression.BITMAP})


def main():
    # test_RLE_single_column()
    test_merge()
    # test_sort()
    # test_bitmap_single_column()
    # test_read_csv_compressions_people()

    # print("***********")
    # column_store_table = _read_csv(
    #     "https://raw.githubusercontent.com/toddwschneider/nyc-taxi-data/master/data/central_park_weather.csv"
    # )
    # # print(column_store_table.to_row_format())
    # # column_store_table.to_csv("out.csv")

    # filt = lambda x: x == "USW00094728"
    # column_store_table.filter("STATION", filt)
    # column_store_table.to_csv("out2.csv")

    # filter_exp = lambda x: x == "2009-01-01"
    # column_store_table.filter("DATE", filter_exp)
    # column_store_table.to_csv("out3.csv")
    pass


if __name__ == "__main__":
    main()
