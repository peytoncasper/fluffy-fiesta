# Instructions

This directory is designed to showcase a potential bug when the token range is shifting and the driver is paging across the token range. Typical scenarios usually utilize this method when doing full table scans such as an integration with Apache Spark or similar.

### Utilize this command to write 40,000 records to Cassandra with the example schema.
`python token_range_test.py write`

### Utilize this command to manually page through 40,000 records by scanning through the token range and requesting 50 records at a time.
`python token_range_test.py read`

### Utilize this command to page through 40,000 records by using the native DSE Driver paging capability.
`python token_range_test.py read_driver_paging`

### Utilize this command to validate the length of result file. This command accepts a second argument of a file name that is produced. This would either be results.csv or results_driver_paging.csv.
`python token_range_test.py validate_length results.csv`

### Utilize this command to verify that all numbers exist. This command accepts a second argument of a file name that is produced. This would either be results.csv or results_driver_paging.csv.
`python token_range_test.py validate_numbers results.csv`