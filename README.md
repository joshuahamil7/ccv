# ccv

Work with CSV and Excel files from the command line. Fast. No spreadsheet needed.

## Quick Start

```bash
# Look at a file
ccv transactions.csv | head

# Find big transactions
ccv transactions.csv filter "amount > 1000" > large.csv

# Pick specific columns
ccv transactions.csv select Date Description Amount > summary.csv

# Pull data from Excel
ccv report.xlsx sheet "March" range "A1:F100" > march.csv

# Run a total
ccv --query "SELECT SUM(amount) FROM data" sales.csv
```

## Install

```bash
git clone https://github.com/joshuahamil7/ccv.git
cd ccv
gem install duckdb nokogiri rubyzip
chmod +x ccv
sudo cp ccv /usr/local/bin/
```

## Why Use It?

· Handles files Excel can't open (millions of rows)
· Script monthly tasks instead of clicking around
· Works with CSVs, Excel files, bank statements

## Need Help?

```bash
ccv --help
```

---

[Download latest release](https://github.com/joshuahamil7/ccv/releases/latest)
