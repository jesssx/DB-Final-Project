# DB-Final-Project

## Datasets

The NYC dataset on Central Park weather can be found [here](https://raw.githubusercontent.com/toddwschneider/nyc-taxi-data/master/data/central_park_weather.csv).

The large customers, people datasets can be generated with the following [public script] (https://www.datablist.com/learn/csv/download-sample-csv-files?fbclid=IwAR3nUpT-So7k9LaS1ekVPpFlAMof-JD23zpjFChOd64OOY_cO82OY9kNnSk).

## Benchmarking Scripts

Run the benchmarking script for our column-oriented algorithms with `python3 benchmarks.py`. This runs each benchmark 5 times, and returns the results in unstructured txt files.

Run the benchmarking script for the pandas benchmarks with `python3 pandas_benchmarks.py`. This also runs each benchmark 5 times, and returns the results in unstructured txt files.

Run the correctness tests with `python3 test.py`. This will print table output for each function that should be manually checked for correctness.

## Acknowledgements

Thanks to all of the 6.5830 staff for teaching us about databases! We learned a lot :)