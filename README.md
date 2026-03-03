# ccv

CCV is a simple but powerful tool for console that helps you work with financial data files—like CSV exports from QuickBooks, Excel spreadsheets, or bank statements—right from your computer's command line. No need to open a heavy spreadsheet program; you can quickly see, filter, clean, and combine your data with just a few words.

---

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

What Can You Do With CCV?

Look inside any data file instantly, even if it's too big for Excel.

```bash
ccv sales_2025.csv
```

Find and save only the rows you need – for example, show all transactions over $1,000.

```bash
ccv transactions.csv filter "amount > 1000" > big_payments.csv
# or use column letters
ccv transactions.csv filter "B > 1000" > big_payments.csv
```

Keep only the columns you need – perfect for stripping down complex reports.

```bash
ccv sales.csv select Customer Amount Date > summary.csv
# or use column indices
ccv sales.csv select column0 column2 column4 > summary.csv
```

Clean messy data – remove duplicates, fill missing values, drop empty rows.

```bash
ccv transactions.csv dedup fill-missing 0 > clean_transactions.csv
```

Run calculations using plain SQL – total sales per customer, average invoice amount, etc.

```bash
ccv --query "SELECT customer, SUM(amount) FROM data GROUP BY customer" sales.csv > totals.csv
```

Work with Excel files like a pro – read specific sheets/ranges, append data, write to cells.

```bash
# Read a specific sheet and range
ccv financials.xlsx sheet "Q1" range "A2:G100" > q1_data.csv

# Append data to existing Excel file
ccv new_data.csv to report.xlsx sheet "Data" mode append

# Write to a specific starting cell
ccv processed.csv to template.xlsx sheet "Results" start B10
```

Parse XML data – from bank statements, export files, or web services.

```bash
ccv bank_statement.xml record_path "//transaction" > transactions.csv
```

Convert between formats seamlessly – CSV, Excel, JSON, Parquet, TSV, and more.

```bash
# Convert between formats
ccv data.xlsx > data.csv
ccv data.csv to output.xlsx
ccv data.csv to output.jsonl
```

---

Why Bookkeepers & Accountants Love It

· Speed: Processes millions of rows in seconds using the high-performance DuckDB engine underneath.
· Automation: Write simple scripts to repeat monthly tasks like preparing client reports or reconciling bank feeds.
· No more "file too large" errors – CCV handles files of any size with streaming, no crashing.
· Interactive exploration: Test filters and calculations step by step before saving results.
· In-place Excel updates: Modify existing workbooks without rebuilding them from scratch.

---

Installation

Prerequisites

· Ruby 2.7 or higher
· Required gems: duckdb, nokogiri, rubyzip

Quick Install

```bash
git clone https://github.com/joshuahamil7/ccv.git
cd ccv
# Install dependencies
gem install duckdb nokogiri rubyzip
# Make executable available
chmod +x ccv
sudo cp ccv /usr/local/bin/
```

---

Usage Examples

Basic Commands

```bash
# View first few rows
ccv transactions.csv | head

# Count rows
ccv transactions.csv | wc -l

# Filter with conditions
ccv transactions.csv filter "date >= '2025-01-01' AND amount < 0"

# Select specific columns
ccv transactions.csv select Date Description Amount

# Clean data
ccv messy.csv dedup fill-missing 0 drop-empty > clean.csv

# Limit output
ccv large_file.csv limit 100 > sample.csv
```

SQL Queries

```bash
# Aggregations
ccv --query "SELECT customer, COUNT(*), SUM(amount) FROM data GROUP BY customer" sales.csv

# Subqueries (using DuckDB features)
ccv --query "SELECT * FROM data WHERE amount > (SELECT AVG(amount) FROM data)" sales.csv

# Date filtering
ccv --query "SELECT * FROM data WHERE date BETWEEN '2025-01-01' AND '2025-03-31'" q1.csv
```

Excel Operations

```bash
# Read specific sheet
ccv report.xlsx sheet "Annual Summary"

# Read cell range
ccv report.xlsx range "B2:F100"

# Append data to existing sheet
ccv new_data.csv to existing.xlsx sheet "Transactions" mode append

# Write to specific starting cell
ccv results.csv to template.xlsx sheet "Output" start C5

# Create new Excel file
ccv data.csv to report.xlsx sheet "Data" start A1
```

XML Processing

```bash
# Parse XML with custom record path
ccv bank_export.xml record_path "//record" > data.csv

# Process nested XML structures
ccv complex.xml record_path "//root/items/item" > items.csv
```

Pipeline Operations

```bash
# Chain operations
ccv sales.csv filter "amount > 1000" select Customer Amount Date limit 50 > top_sales.csv

# Read from stdin, write to stdout
cat data.csv | ccv filter "column2 != ''" > cleaned.csv

# Multiple transformations
ccv data.csv filter "status == 'active'" select name email balance limit 1000 to active_users.xlsx sheet "Active Users" start A1
```

---

Column References Made Simple

Any of these work interchangeably – use whatever's most convenient:

· Original column name: filter "Amount > 1000"
· Column letter: filter "B > 1000" (A=1, B=2, etc.)
· Column index: filter "column1 > 1000" (0-based: column0 = first column)

---

Command Reference

Input Options

· from <file> - Input file (or use as first argument)
· sheet <name> - Excel sheet name
· range <A1:B10> - Excel cell range
· record_path <xpath> - XML record path
· --no-headers - Treat first row as data, not headers

Output Options

· to <file> - Output file (default: stdout)
· sheet <name> - Output Excel sheet name
· start <A1> - Starting cell for Excel output
· mode [append|overwrite] - Excel write mode (default: append)

Transformations

· select <columns> - Keep only specified columns
· filter <condition> - Keep rows matching condition
· dedup - Remove duplicate rows
· fill-missing <value> - Replace empty cells with value
· drop-empty - Remove rows that are completely empty
· limit <n> - Show only first n rows
· -q, --query <sql> - Run SQL query

---

Real-World Workflows

Monthly Bank Reconciliation

```bash
# Extract bank transactions for March
ccv bank_statement.xlsx sheet "March" range "A5:F200" > march_raw.csv

# Filter to find large transactions
ccv march_raw.csv filter "abs(amount) > 10000" > large_transactions.csv

# Match against QuickBooks export
ccv --query "
  SELECT b.date, b.description, b.amount, q.invoice 
  FROM 'bank.csv' b 
  LEFT JOIN 'quickbooks.csv' q ON b.amount = q.amount 
  WHERE q.invoice IS NULL
" > unmatched.csv
```

Client Report Generation

```bash
# Extract client data from master file
ccv all_clients.xlsx sheet "Data" filter "client_id == 'ACME123'" > acme_raw.csv

# Clean and aggregate
ccv acme_raw.csv dedup fill-missing 0 --query "SELECT date, SUM(amount) as daily_total FROM data GROUP BY date" > acme_daily.csv

# Write to report template
ccv acme_daily.csv to acme_report.xlsx sheet "Daily Totals" start B5
```

Batch Processing Multiple Files

```bash
# Process all CSV files in a folder
for file in *.csv; do
  ccv "$file" filter "amount > 0" > "positive_$file"
done

# Combine multiple files
ccv --query "SELECT * FROM 'jan.csv' UNION ALL SELECT * FROM 'feb.csv'" > q1.csv
```

---

Tips & Troubleshooting

File too large? CCV streams data—it never loads everything into memory. Files with millions of rows work fine.

Encoding issues? Files are assumed UTF-8. For Excel files with odd characters, try saving as CSV first.

Need headers? CCV preserves headers by default. Use --no-headers if your file lacks them.

Column names with spaces? Use backticks: filter "`Transaction Amount` > 1000"

Performance tip: For repeated queries on the same file, CCV caches the data in-memory after first read.

---

Get Help

```bash
ccv --help
```

---

License

CCV is free and open-source software. Use it, modify it, share it.

---

Contributing

Report issues or suggest features on GitHub.

---

Built to make your daily data work easier.
