# ccv
CCV is a console utility, a Swiss Army knife for tabular data manipulation, supporting CSV, XLSX, JSON, Parquet, and TSV formats. It uses DuckDB as a backend for high-performance SQL operations on files.

# CCV - Command-line Data Swiss Army Knife v3.0.0

CCV (CSV/Excel/Data Utility) is a powerful command-line tool for data manipulation, analysis, and transformation. Version 3.0.0 introduces an interactive Ruby DSL console and enhanced transformation capabilities.

## Features

- **Multi-format support**: CSV, Excel (XLSX/XLS), JSON
- **Interactive console**: Ruby DSL for exploratory data analysis
- **SQL-powered queries**: Use DuckDB's full SQL capabilities
- **Data cleaning**: Drop nulls, fill values, remove duplicates
- **Excel operations**: Read/write specific ranges, sheets
- **Data transformation**: Filter, sort, group, aggregate, pivot
- **Chainable interface**: Build complex pipelines easily

## Examples

### View a CSV file
ccv data.csv

### Show statistics
ccv --stats sales.xlsx

### Filter and export
ccv --filter "amount > 100" --select "date,product" data.csv --output filtered.csv

### SQL query
ccv --query "SELECT * FROM $1 WHERE category = 'Electronics'" products.csv

## Excel

### Read specific Excel range
ccv --sheet "Q1" --range "A2:G100" financials.xlsx --output q1_data.csv

### Append to existing Excel
ccv --append report.xlsx --sheet "Data" new_data.csv

### Write to specific location
ccv --write template.xlsx --range "B10" --sheet "Results" processed.csv

## Requirements

Ruby 2.7 or higher
DuckDB gem (gem install duckdb)
Optional: rubyXL gem for Excel support (gem install rubyXL)

## Quick Start

### Installation

```bash
# Clone repository
git clone https://github.com/joshuahamil7/ccv.git
cd ccv

# Install as system utility
chmod +x install.sh
sudo ./install.sh

# Or use directly
chmod +x ccv.rb
./ccv.rb --help
