# Data Quality Workflow

I would like to implement data quality check methods to ensure data accuracy, completeness, reliability, and consistency.

## Features

### Preparations
1. Concat primary key columns of source and destination tables, then hash them to get one single column, called "primary_key" column.
2. Concat other columns of source and destination tables, then hash them to get one single column, called "hash_key" column.

### Start
- Check for missing records
  - The records whose primary_key columns are in source but not in destination -> MISSED
- Check for invalid records
  - The records whose primary_key columns are in both source and destination, but hash_key columns are not the same >>> INVALID
