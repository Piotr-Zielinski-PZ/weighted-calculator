# Weighted Calculator

A Python tool for calculating weighted averages from CSV data, with support for both plain text and LZ4-compressed files.

## Overview

This project provides a data processing module that loads CSV data from various sources, parses it, and calculates weighted averages for rows matching user-specified search criteria.

## Features

- **Multiple Data Sources**: Supports plain text CSV files and LZ4-compressed files
- **Factory Pattern**: Automatically selects the appropriate data loader based on file extension
- **Cached Parsing**: Uses LRU caching for efficient CSV parsing
- **Optimized Lookup**: Hash-based O(1) lookup for matching rows
- **Weighted Averages**: Calculates weighted averages where:
  - Odd values have weight **10**
  - Even values have weight **20**

## Architecture

The project follows clean architecture principles:

- **Data Layer**: `DataLoader` abstract class with `LZ4DataLoader` and `PlainDataLoader` implementations
- **Domain Layer**: `SearchCriteria` and `WeightedValue` value objects
- **Service Layer**: `DataParser`, `WeightedAverageCalculator`, and `Task2Service` orchestrator

## Sample Data Format

The input CSV file should have the following structure:

```csv
a,b,c,d,value
862984,29105,605280,678194,302120
20226,781899,186952,506894,325696
```

- Columns `a`, `b`, `c`, `d`, `e` are search key columns
- Column `value` is the numeric value to be averaged
- The actual data file is compressed with LZ4: `data/find_match_average_v2.dat.lz4`

### Example Search Criteria

```python
SEARCH_LIST = [
    {'a': 862984, 'b': 29105, 'c': 605280, 'd': 678194, 'e': 302120},
    {'a': 20226, 'b': 781899, 'c': 186952, 'd': 506894, 'e': 325696}
]
```

## Usage

### Running the Script

```bash
python main.py
```

This will load the default data file `data/find_match_average_v2.dat.lz4` and calculate the weighted average for the predefined search criteria.

### Using as a Module

```python
from main import task2

# Define search criteria
search_list = [
    {'a': 862984, 'b': 29105, 'c': 605280, 'd': 678194, 'e': 302120},
    {'a': 20226, 'b': 781899, 'c': 186952, 'd': 506894, 'e': 325696}
]

# From file (LZ4 compressed)
result = task2(search_list, "data/find_match_average_v2.dat.lz4")
print(f"Weighted average: {result}")

# Or from raw CSV string
csv_data = """a,b,c,d,e,value
862984,29105,605280,678194,302120,100
20226,781899,186952,506894,325696,200"""

result = task2(search_list, csv_data)
print(f"Weighted average: {result}")
```

## Requirements

- Python 3.7+
- `lz4` command-line tool (for LZ4 file support)

Install lz4 via Homebrew:
```bash
brew install lz4
```

## Output

The function returns a string representing the weighted average rounded to 1 decimal place, e.g., `"123.4"`.
