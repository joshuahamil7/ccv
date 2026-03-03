# ccv

CCV is a simple but powerful tool that helps you work with financial data files—like CSV exports from QuickBooks, Excel spreadsheets, or bank statements—right from your computer’s command line. No need to open a heavy spreadsheet program; you can quickly see, filter, clean, and combine your data with just a few words.

What Can You Do With CCV?

· Look inside any data file instantly, even if it’s too big for Excel.
  ```
  ccv sales_2025.csv
  ```
· Find and save only the rows you need – for example, show all transactions over $1,000.
  ```
  ccv --filter "amount > 1000" transactions.csv --output big_payments.csv
  ```
· Run simple calculations – total sales per customer, average invoice amount, etc. – using plain English-like SQL.
  ```
  ccv --query "SELECT customer, SUM(amount) FROM $1 GROUP BY customer" sales.csv --output totals.csv
  ```
· Clean messy data – remove duplicate entries, fill in missing cells, or drop empty rows in one step.
  ```
  ccv --dedup --fill-missing 0 transactions.csv --output clean_transactions.csv
  ```
· Work with Excel files like a pro – read a specific range from a sheet, append new data to an existing workbook, or write results to a particular cell.
  ```
  ccv sheet "Q1" --range "A2:G100" financials.xlsx --output q1_data.csv
  ccv --append report.xlsx --sheet "Data" new_data.csv
  ccv --write template.xlsx --range "B10" --sheet "Results" processed.csv
  ```
· Handle many file types – CSV, Excel (XLSX/XLS), JSON, Parquet, and TSV – all in one place.

Why Bookkeepers & Accountants Love It

· Speed: Processes millions of rows in seconds, thanks to the high-performance DuckDB engine underneath.
· Automation: You can write simple scripts to repeat monthly tasks, like preparing client reports or reconciling bank feeds.
· No more “file too large” errors – CCV handles files of any size without crashing.
· Interactive mode lets you explore data step by step, testing filters and calculations before saving.


Install (requires Ruby 2.7+):
   ```
   git clone https://github.com/joshuahamil7/ccv.git
   cd ccv
   sudo ./install.sh
   ```

Quick Start

1. Look at a file — see what you're working with

```bash
ccv bank_statement.csv | head -5
```

2. Find specific transactions — filter by amount, date, or description

```bash
ccv transactions.csv filter "amount > 5000" > large_payments.csv
```

3. Pull data from Excel — grab a specific sheet or range

```bash
ccv client_report.xlsx sheet "March" range "A10:F50" > march_data.csv
```

4. Run a quick total — sum it up in seconds

```bash
ccv --query "SELECT SUM(amount) FROM data" sales.csv
```

5. Save results back to Excel — update existing files without rebuilding them

```bash
ccv processed.csv to template.xlsx sheet "Results" start B2
```

That's it. You're up and running in under a minute.

---

Get help anytime:
   ```
   ccv --help
   ```

CCV is free, open‑source, and built to make your daily data work easier. Give it a try!
