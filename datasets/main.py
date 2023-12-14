import csv
from pathlib import Path
from zipfile import ZipFile, ZIP_BZIP2

from generators import TYPES_TO_GENERATORS
from schemas import (
    PEOPLE_SCHEMA,
    COMPANY_SCHEMA,
)

SCHEMA_TO_DICT = {
    "people": PEOPLE_SCHEMA,
    "companies": COMPANY_SCHEMA,
}


def generate_file(schema="customers", name="customers", count=1000000):
    p = Path(__file__).parent / "files/{}".format(schema)
    p.mkdir(parents=True, exist_ok=True)

    file_name = "{}.csv".format(name)
    file_path = p / file_name

    if not file_path.exists():
        schema_dict = SCHEMA_TO_DICT[schema]

        with open(file_path, "w", newline="") as file:
            writer = csv.writer(file)

            # Headers
            headers = [elem["name"] for elem in schema_dict]
            headers.insert(0, "Index")  # Add an Index header
            writer.writerow(headers)

            # Content
            data_generators = [
                TYPES_TO_GENERATORS[elem["type"]] for elem in schema_dict
            ]

            rows = []
            for index in range(1, count + 1):
                row = [gen() for gen in data_generators]
                row.insert(0, index)
                rows.append(row)

                if index % 1000 == 0:
                    writer.writerows(rows)
                    rows = []

                if index % 10000 == 0:
                    print("{}/{}".format(index, count))

            writer.writerows(rows)
    else:
        print("{} already exists".format(file_path))

    # Create a zip version
    file_name_zip = "{}.zip".format(name)
    file_path_zip = p / file_name_zip
    if not file_path_zip.exists():
        with ZipFile(file_path_zip, "w", ZIP_BZIP2) as zipObj:
            zipObj.write(filename=file_path, arcname=file_name)


if __name__ == "__main__":
    # generate_file("people", "people-100", 100)
    # generate_file('people', 'people-1000', 1000)
    # generate_file("people", "people-10000", 10000)
    # generate_file("people", "people-100000", 100000)
    # generate_file('people', 'people-500000', 500000)
    # generate_file("people", "people-1000000", 1000000)
    # generate_file("people", "people-10000000", 10000000)
    # generate_file("people", "people-100000000", 100000000)

    # generate_file("companies", "companies-20", 20)
    pass
