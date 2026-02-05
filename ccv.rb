#!/usr/bin/env ruby
# frozen_string_literal: true
# author: JH
# update: 2026-Feb-5
# email:  joshuahamil7@runbox.com

require 'csv'
require 'duckdb'
require 'optparse'
require 'tempfile'
require 'fileutils'
require 'json'
require 'set'
require 'forwardable'

# Main CCV class with DSL support
class CCV
  VERSION = "3.0.0".freeze
  
  # Configuration container
  class Config
    attr_accessor :input, :output, :format, :sheet, :range, :debug, :stats,
                  :combine, :check, :clear, :append, :write, :query, :query_file,
                  :distinct, :filter, :select, :sort, :limit, :group_by, :aggregate,
                  :join, :union, :pivot, :sample, :dropna, :fillna, :rename, :cast,
                  :input_files, :where
    
    def initialize(**kwargs)
      @input_files = []
      kwargs.each { |k, v| send("#{k}=", v) if respond_to?("#{k}=") }
    end
    
    def transform?
      query || query_file || 
      distinct || filter || select || sort || limit || group_by || aggregate ||
      join || union || pivot || sample || dropna || fillna || rename || cast ||
      combine || stats
    end
    
    def to_h
      {
        input: input, output: output, format: format, sheet: sheet, range: range,
        debug: debug, stats: stats, combine: combine, check: check, clear: clear,
        append: append, write: write, query: query, query_file: query_file,
        distinct: distinct, filter: filter, select: select, sort: sort, limit: limit,
        group_by: group_by, aggregate: aggregate, join: join, union: union,
        pivot: pivot, sample: sample, dropna: dropna, fillna: fillna,
        rename: rename, cast: cast, input_files: input_files, where: where
      }.compact
    end 
  end

  # Result container with chainable methods
  class Dataset
    extend Forwardable
    include Enumerable
    
    attr_reader :columns
    
    def_delegators :@data, :each, :size, :empty?, :first, :last
    
    def initialize(data = [], columns = nil)
      @data = data
      @columns = columns || (data.first ? (0...data.first.size).map { |i| "col#{i}" } : [])
    end
    
    def to_a
      @data
    end
    
    def to_csv
      CSV.generate do |csv|
        csv << @columns
        @data.each { |row| csv << row }
      end
    end
    
    def to_json
      @data.map do |row|
        @columns.zip(row).to_h
      end.to_json
    end
    
    def column_index(name)
      @columns.index(name.to_s)
    end
    
    def method_missing(name, *args, &block)
      idx = column_index(name)
      return @data.map { |row| row[idx] } if idx
      super
    end
    
    def respond_to_missing?(name, include_private = false)
      column_index(name) || super
    end
    
    def inspect
      "#<Dataset rows=#{size} columns=#{@columns.size}>"
    end
    
    def column_names
      @columns
    end
  end

  # DSL for console operations
  module DSL
    def from(source, **options)
      @current_dataset = case source
        when String then CCV.read(source, **options)
        when Array  then Dataset.new(source)
        else source
      end
      self
    end
    
    def select(*columns)
      @current_dataset = CCV.transform(@current_dataset, select: columns.join(', '))
      self
    end
    
    def where(condition)
      @current_dataset = CCV.transform(@current_dataset, filter: condition)
      self
    end
    
    def order(*columns)
      @current_dataset = CCV.transform(@current_dataset, sort: columns.join(','))
      self
    end
    
    def limit(n)
      @current_dataset = CCV.transform(@current_dataset, limit: n)
      self
    end
    
    def group(*columns)
      @current_dataset = CCV.transform(@current_dataset, group_by: columns.join(','))
      self
    end
    
    def aggregate(expr)
      @current_dataset = CCV.transform(@current_dataset, aggregate: expr)
      self
    end
    
    def distinct
      @current_dataset = CCV.transform(@current_dataset, distinct: true)
      self
    end
    
    def sample(n)
      @current_dataset = CCV.transform(@current_dataset, sample: n)
      self
    end
    
    def dropna
      @current_dataset = CCV.transform(@current_dataset, dropna: true)
      self
    end
    
    def rename(**mapping)
      @current_dataset = CCV.transform(@current_dataset, rename: mapping.to_json)
      self
    end
    
    def cast(**mapping)
      @current_dataset = CCV.transform(@current_dataset, cast: mapping.to_json)
      self
    end
    
    def fillna(value)
      @current_dataset = CCV.transform(@current_dataset, fillna: value)
      self
    end
    
    def to_csv(file = nil)
      CCV.export(@current_dataset, file, format: :csv)
    end
    
    def to_json(file = nil)
      CCV.export(@current_dataset, file, format: :json)
    end
    
    def to_excel(file = nil, **options)
      CCV.export(@current_dataset, file, { format: :xlsx }.merge(options))
    end
    
    def result
      @current_dataset
    end
    
    def stats
      CCV.stats(@current_dataset)
    end
    
    def peek(n = 5)
      CCV.peek(@current_dataset, n)
    end
  end

  class << self
    include DSL
    
    # Main entry points
    def run(args = ARGV)
      config = parse_args(args)
      execute(config)
    rescue => e
      warn "Error: #{e.message}" if config&.debug
      warn e.backtrace if config&.debug
      exit 1
    end
    
    def read(source, **options)
      case source
      when '-'
        read_stdin(**options)
      when /\.csv$/i
        read_csv(source, **options)
      when /\.(xlsx?|xls)$/i
        read_excel(source, **options)
      when /\.json$/i
        read_json(source)
      else
        read_any(source, **options)
      end
    end
    
    def transform(dataset, **options)
      return dataset unless dataset.is_a?(Dataset)
      
      puts "DEBUG: Transform options: #{options}" if options[:debug]
      
      # Handle query with placeholders
      if options[:query]
        sql = options[:query]
        # Replace $1, $2, etc. with actual file paths
        if options[:input_files]
          options[:input_files].each_with_index do |file, i|
            sql = sql.gsub("$#{i + 1}", "'#{escape(file)}'")
          end
        end
        puts "DEBUG: Query SQL: #{sql}" if options[:debug]
        return execute_sql(sql)
      end
      
      # Handle query file
      if options[:query_file]
        sql = File.read(options[:query_file])
        if options[:input_files]
          options[:input_files].each_with_index do |file, i|
            sql = sql.gsub("$#{i + 1}", "'#{escape(file)}'")
          end
        end
        puts "DEBUG: Query File SQL: #{sql}" if options[:debug]
        return execute_sql(sql)
      end
      
      # Handle special transformations
      if options[:rename]
        dataset = rename_columns(dataset, options[:rename])
      end
      
      if options[:dropna]
        dataset = dropna(dataset)
      end
      
      if options[:sample]
        dataset = sample_rows(dataset, options[:sample])
      end
      
      if options[:fillna]
        dataset = fillna(dataset, options[:fillna])
      end
      
      if options[:cast]
        dataset = cast_types(dataset, options[:cast])
      end
      
      # Apply SQL transformations
      sql_options = options.reject { |k, v| 
        [:rename, :dropna, :sample, :fillna, :cast, :query, :query_file, :input_files, :debug].include?(k) || v.nil? 
      }
      
      if sql_options.any?
        puts "DEBUG: SQL options: #{sql_options}" if options[:debug]
        dataset = apply_sql_transform(dataset, **sql_options)
      end
      
      dataset
    rescue => e
      warn "Transform failed: #{e.message}" if options[:debug]
      warn "Backtrace: #{e.backtrace}" if options[:debug]
      dataset
    end
    
    def export(dataset, file = nil, **options)
      return unless dataset.is_a?(Dataset)
      
      case options[:format] || format_from_file(file)
      when :csv, 'csv'
        export_csv(dataset, file)
      when :xlsx, 'xlsx', :excel, 'excel'
        export_excel(dataset, file, **options)
      when :json, 'json'
        export_json(dataset, file)
      else
        export_csv(dataset, file)
      end
    end
    
    def stats(dataset)
      return unless dataset.is_a?(Dataset)
      
      puts "Dataset Statistics"
      puts "=" * 80
      puts "Rows: #{dataset.size}"
      puts "Columns: #{dataset.columns.size}"
      puts "Columns: #{dataset.columns.join(', ')}"
      puts
      
      dataset.columns.each_with_index do |col, i|
        values = dataset.map { |row| row[i] }
        numeric = values.select { |v| v.is_a?(Numeric) || v.to_s =~ /^\d+(\.\d+)?$/ }
        
        puts "#{col}:"
        puts "  Count: #{values.size}"
        puts "  Nulls: #{values.count { |v| v.nil? || v.to_s.strip.empty? }}"
        puts "  Unique: #{values.compact.uniq.size}"
        
        if numeric.any?
          numeric_vals = numeric.map { |v| v.to_f }
          puts "  Min: #{numeric_vals.min}"
          puts "  Max: #{numeric_vals.max}"
          puts "  Mean: #{numeric_vals.sum.to_f / numeric_vals.size}"
          puts "  Sum: #{numeric_vals.sum}"
        end
        
        # Show sample values
        sample_vals = values.reject { |v| v.nil? || v.to_s.strip.empty? }.first(5).map(&:to_s)
        if sample_vals.any?
          puts "  Sample: #{sample_vals.join(', ')}"
        end
        
        puts
      end
    end
    
    def peek(dataset, n = 5)
      return unless dataset.is_a?(Dataset)
      
      puts "First #{n} rows:"
      puts dataset.columns.join("\t")
      puts "-" * 80
      dataset.first(n).each do |row|
        puts row.map { |v| v.to_s[0..30] }.join("\t")
      end
      nil
    end
    
    public
    
    # Configuration parsing
    def parse_args(args)
      config = Config.new
      
      OptionParser.new do |opts|
        opts.banner = "Usage: ccv [options] [files...]"
        
        opts.on("--combine") { config.combine = true }
        opts.on("--check") { config.check = true }
        opts.on("--clear FILE") { |f| config.clear = f }
        opts.on("--append FILE") { |f| config.append = f }
        opts.on("--write FILE") { |f| config.write = f }
        
        opts.on("-q", "--query SQL") { |s| config.query = s }
        opts.on("--query-file FILE") { |f| config.query_file = f }
        
        opts.on("--distinct") { config.distinct = true }
        opts.on("--filter EXPR", "--where EXPR") { |e| config.filter = e }
        opts.on("--select COLS") { |c| config.select = c }
        opts.on("--sort COLS") { |c| config.sort = c }
        opts.on("--limit N", Integer) { |n| config.limit = n }
        opts.on("--group-by COLS") { |c| config.group_by = c }
        opts.on("--aggregate EXPR") { |e| config.aggregate = e }
        opts.on("--join SPEC") { |s| config.join = s }
        opts.on("--union [ALL]") { |a| config.union = a || true }
        opts.on("--pivot SPEC") { |s| config.pivot = s }
        opts.on("--sample N", Integer) { |n| config.sample = n }
        opts.on("--dropna") { config.dropna = true }
        opts.on("--fillna VALUE") { |v| config.fillna = v }
        opts.on("--rename MAP") { |m| config.rename = m }
        opts.on("--cast MAP") { |m| config.cast = m }
        
        opts.on("--sheet NAME") { |n| config.sheet = n }
        opts.on("--range RANGE") { |r| config.range = r }
        opts.on("--input FILE") { |f| config.input = f }
        opts.on("--output FILE") { |f| config.output = f }
        opts.on("--format FORMAT") { |f| config.format = f }
        
        opts.on("--stats") { config.stats = true }
        opts.on("--debug") { config.debug = true }
        opts.on("--peek [N]", Integer) { |n| config.limit = n || 5 }
        opts.on("-h", "--help") { show_help; exit 0 }
      end.parse!(args)
      
      config.input_files = args
      config
    end
    
    private
    
    def show_help
      puts <<~HELP
        CCV #{VERSION} - Command-line Data Swiss Army Knife
        
        Common Usage:
          ccv data.csv                         # View CSV
          ccv --query "SELECT * FROM $1" file  # SQL query
          ccv --filter "amount > 100" data     # Filter rows
          ccv --stats sales.xlsx               # Show stats
          ccv --select name,amount data.csv    # Select columns
          ccv --sort -amount data.csv          # Sort descending
          ccv --limit 10 data.csv              # Limit rows
          ccv --distinct data.csv              # Unique rows
          ccv --group-by category --aggregate "SUM(amount)" data.csv
          
        Filter Examples:
          --filter "amount > 100"              # Greater than
          --filter "name = 'John'"             # Equal to string
          --filter "date >= '2023-01-01'"      # Date comparison
          --filter "status IN ('active', 'pending')" # Multiple values
          --filter "name LIKE '%Smith%'"       # Pattern match
          --filter "amount IS NOT NULL"        # Not null
          --filter "category = 'A' AND amount > 100" # Multiple conditions
        
        DSL Examples in Ruby:
          CCV.from("data.csv")
            .where("amount > 100")
            .select(:date, :product)
            .to_csv("filtered.csv")
          
          CCV.from("sales.csv")
            .group(:region)
            .aggregate("SUM(revenue) as total_revenue")
            .order("-total_revenue")
            .to_json
      HELP
    end
    
    def execute(config)
      @config = config
      
      if config.check
        check_mode(config)
      elsif config.clear
        clear_mode(config)
      elsif config.append
        append_mode(config)
      elsif config.write
        write_mode(config)
      elsif config.transform?
        transform_mode(config)
      else
        default_mode(config)
      end
    end
    
    # File reading
    def read_csv(file, **options)
      with_duckdb do |db|
        result = db.query("SELECT * FROM read_csv_auto('#{escape(file)}')")
        Dataset.new(result.to_a, result.columns.map(&:name))
      end
    rescue => e
      warn "Failed to read CSV: #{e.message}" if options[:debug]
      Dataset.new
    end
    
    def read_excel(file, sheet: nil, range: nil, **options)
      with_duckdb do |db|
        sql_parts = ["SELECT * FROM read_xlsx('#{escape(file)}'"]
        params = []
        params << "sheet='#{escape(sheet)}'" if sheet
        params << "range='#{range}'" if range
        
        if params.any?
          sql_parts << ", " + params.join(", ")
        end
        sql_parts << ")"
        
        result = db.query(sql_parts.join)
        Dataset.new(result.to_a, result.columns.map(&:name))
      end
    rescue => e
      warn "Failed to read Excel: #{e.message}" if options[:debug]
      Dataset.new
    end
    
    def read_json(file, **options)
      with_duckdb do |db|
        result = db.query("SELECT * FROM read_json_auto('#{escape(file)}')")
        Dataset.new(result.to_a, result.columns.map(&:name))
      end
    rescue => e
      warn "Failed to read JSON: #{e.message}" if options[:debug]
      Dataset.new
    end
    
    def read_stdin(**options)
      content = $stdin.read
      return Dataset.new if content.empty?
      
      tempfile = Tempfile.new(['ccv', '.csv'])
      begin
        tempfile.write(content)
        tempfile.close
        
        if content.strip.start_with?('{', '[')
          read_json(tempfile.path, **options)
        else
          read_csv(tempfile.path, **options)
        end
      ensure
        tempfile.unlink
      end
    end
    
    def read_any(file, **options)
      [method(:read_csv), method(:read_excel), method(:read_json)].each do |reader|
        result = reader.call(file, **options)
        return result unless result.empty?
      end
      Dataset.new
    end
    
    # Special transformation methods
    def rename_columns(dataset, rename_map_str)
      begin
        rename_map = if rename_map_str.is_a?(Hash)
                      rename_map_str
                    else
                      JSON.parse(rename_map_str)
                    end
        
        new_columns = dataset.columns.map do |col|
          rename_map[col.to_s] || col
        end
        Dataset.new(dataset.to_a, new_columns)
      rescue JSON::ParserError => e
        warn "Invalid rename JSON: #{e.message}"
        dataset
      end
    end
    
    def dropna(dataset)
      filtered_data = dataset.select do |row|
        !row.any?(&:nil?)
      end
      Dataset.new(filtered_data, dataset.columns)
    end
    
    def fillna(dataset, value)
      filled_data = dataset.map do |row|
        row.map { |cell| cell.nil? ? value : cell }
      end
      Dataset.new(filled_data, dataset.columns)
    end
    
    def cast_types(dataset, cast_map_str)
      begin
        cast_map = JSON.parse(cast_map_str)
        # For now, just validate JSON - actual casting would need more work
        dataset
      rescue JSON::ParserError => e
        warn "Invalid cast JSON: #{e.message}"
        dataset
      end
    end
    
    def sample_rows(dataset, n)
      n = [n, dataset.size].min
      sampled_data = dataset.to_a.sample(n)
      Dataset.new(sampled_data, dataset.columns)
    end
    
    # SQL transformation pipeline
    def apply_sql_transform(dataset, **options)
      return dataset if dataset.empty?
      
      # Create temporary CSV file
      tempfile = Tempfile.new(['ccv', '.csv'])
      begin
        CSV.open(tempfile.path, 'w') do |csv|
          csv << dataset.columns
          dataset.each { |row| csv << row }
        end
        tempfile.close
        
        # Build and execute SQL
        sql = build_sql_query(tempfile.path, dataset.columns, **options)
        puts "DEBUG: Generated SQL: #{sql}" if options[:debug]
        
        with_duckdb do |db|
          result = db.query(sql)
          Dataset.new(result.to_a, result.columns.map(&:name))
        end
      rescue => e
        warn "SQL transform error: #{e.message}" if options[:debug]
        warn "SQL was: #{sql}" if options[:debug]
        dataset
      ensure
        tempfile.unlink
      end
    end
    
    def build_sql_query(filepath, columns, **options)
      parts = ["SELECT"]
      
      # SELECT clause with column name mapping
      if options[:select]
        select_clause = options[:select]
        # Quote column names if they match dataset columns
        columns.each do |col|
          # Only quote column names that are simple identifiers (not expressions)
          if select_clause.include?(col) && !select_clause.include?("#{col}(")
            select_clause = select_clause.gsub(/(?<![\.\w\(])#{Regexp.escape(col)}(?![\.\w\)])/i, "\"#{col}\"")
          end
        end
        parts << select_clause
      elsif options[:aggregate] && options[:group_by]
        aggregate = options[:aggregate]
        group_by = options[:group_by]
        columns.each do |col|
          aggregate = aggregate.gsub(/(?<![\.\w\(])#{Regexp.escape(col)}(?![\.\w\)])/i, "\"#{col}\"")
          group_by = group_by.gsub(/(?<![\.\w\(])#{Regexp.escape(col)}(?![\.\w\)])/i, "\"#{col}\"")
        end
        parts << "#{group_by}, #{aggregate}"
      elsif options[:aggregate]
        aggregate = options[:aggregate]
        columns.each do |col|
          aggregate = aggregate.gsub(/(?<![\.\w\(])#{Regexp.escape(col)}(?![\.\w\)])/i, "\"#{col}\"")
        end
        parts << aggregate
      else
        parts << "*"
      end
      
      # FROM clause
      parts << "FROM read_csv_auto('#{escape(filepath)}')"
      
      # WHERE clause with filter mapping
      if options[:filter]
        filter = options[:filter]
        # Process filter expression
        filter = process_filter_expression(filter, columns)
        parts << "WHERE #{filter}"
      end
      
      # GROUP BY clause
      if options[:group_by] && !options[:aggregate]&.include?('GROUP BY')
        group_by = options[:group_by]
        columns.each do |col|
          group_by = group_by.gsub(/(?<![\.\w\(])#{Regexp.escape(col)}(?![\.\w\)])/i, "\"#{col}\"")
        end
        parts << "GROUP BY #{group_by}"
      end
      
      # ORDER BY clause
      if options[:sort]
        order = options[:sort].split(',').map do |col|
          col = col.strip
          if col.start_with?('-')
            col_name = col[1..-1].strip
            columns.each do |dataset_col|
              if col_name.casecmp?(dataset_col)
                col_name = "\"#{dataset_col}\""
                break
              end
            end
            "#{col_name} DESC"
          else
            col_name = col
            columns.each do |dataset_col|
              if col_name.casecmp?(dataset_col)
                col_name = "\"#{dataset_col}\""
                break
              end
            end
            "#{col_name} ASC"
          end
        end.join(', ')
        parts << "ORDER BY #{order}"
      end
      
      # LIMIT clause
      parts << "LIMIT #{options[:limit]}" if options[:limit]
      
      # DISTINCT
      if options[:distinct]
        parts[0] = "SELECT DISTINCT"
      end
      
      parts.join(' ')
    end
    
    # Process filter expressions from DSL to SQL
    def process_filter_expression(expression, columns)
      return expression if expression.strip.empty?
      
      expr = expression.strip
      
      # Common pattern: just a column name means "column IS NOT NULL"
      if expr =~ /^\w+$/ && columns.include?(expr)
        return "\"#{expr}\" IS NOT NULL"
      end
      
      # Handle DSL shortcuts
      # 1. Ruby-style comparisons: ==, !=, >, <, >=, <=
      expr = expr.gsub(/==/, '=')
      
      # 2. Handle Ruby style 'and'/'or' (convert to SQL AND/OR but be careful)
      expr = expr.gsub(/\band\b/i, 'AND').gsub(/\bor\b/i, 'OR')
      
      # 3. Handle 'nil' -> 'NULL'
      expr = expr.gsub(/\bnil\b/i, 'NULL')
      
      # 4. Quote column names
      columns.each do |col|
        # Skip if column is already quoted or part of a string
        next if expr =~ /['"]/
        
        # Find column names not inside quotes or function calls
        expr = expr.gsub(/(?<![\.\w\(])#{Regexp.escape(col)}(?![\.\w\)])/i) do |match|
          "\"#{col}\""
        end
      end
      
      # 5. Handle string literals - ensure single quotes
      expr = expr.gsub(/"([^"]*)"/, "'\\1'")
      
      # 6. Fix common pattern: column value (without operator) -> column = value
      if expr =~ /^"\w+"\s+\w/
        # Already has operator
      elsif expr =~ /^"\w+"\s+'[^']+'$/
        # Already column = 'value'
      elsif expr =~ /^"\w+"\s+(\d+\.?\d*)$/
        expr = "#{$`}#{$&} = #{$1}"
      end
      
      expr
    end
    
    def execute_sql(sql)
      puts "DEBUG: Executing SQL: #{sql}" if @config&.debug
      with_duckdb do |db|
        result = db.query(sql)
        Dataset.new(result.to_a, result.columns.map(&:name))
      end
    end
    
    # File export
    def export_csv(dataset, file)
      if file
        CSV.open(file, 'w') do |csv|
          csv << dataset.columns
          dataset.each { |row| csv << row }
        end
      else
        puts dataset.to_csv
      end
    end
    
    def export_json(dataset, file)
      json = dataset.to_json
      if file
        File.write(file, json)
      else
        puts json
      end
    end
    
    def export_excel(dataset, file, sheet: 'Sheet1', **options)
      require 'rubyXL'
      
      workbook = RubyXL::Workbook.new
      worksheet = workbook[0]
      worksheet.sheet_name = sheet
      
      dataset.columns.each_with_index do |col, i|
        worksheet.add_cell(0, i, col)
      end
      
      dataset.each_with_index do |row, row_idx|
        row.each_with_index do |cell, col_idx|
          worksheet.add_cell(row_idx + 1, col_idx, cell)
        end
      end
      
      if file
        workbook.write(file)
      else
        Tempfile.open(['ccv', '.xlsx']) do |f|
          workbook.write(f.path)
          print File.binread(f.path)
        end
      end
    rescue LoadError
      warn "Excel export requires rubyXL gem"
      export_csv(dataset, file)
    end
    
    def format_from_file(file)
      return unless file
      
      case File.extname(file).downcase
      when '.csv', '.tsv' then :csv
      when '.xlsx', '.xls' then :xlsx
      when '.json' then :json
      end
    end
    
    # DuckDB management
    def with_duckdb
      db = DuckDB::Database.open
      conn = db.connect
      
      %w[excel json parquet].each do |ext|
        begin
          conn.query("INSTALL #{ext}")
          conn.query("LOAD #{ext}")
        rescue
        end
      end
      
      yield conn
    ensure
      conn&.close
      db&.close
    end
    
    def escape(str)
      str.gsub("'", "''")
    end
    
    # Mode handlers
    def transform_mode(config)
      sources = config.input_files
      sources << config.input if config.input
      
      dataset = if sources.empty? && !$stdin.tty?
                  read('-')
                elsif sources.one?
                  read(sources.first, sheet: config.sheet, range: config.range)
                else
                  datasets = sources.map { |s| read(s) }
                  Dataset.new(
                    datasets.flat_map(&:to_a),
                    datasets.first&.columns || []
                  )
                end
      
      transform_options = config.to_h.compact
      transform_options[:input_files] = sources if config.query
      transform_options[:debug] = config.debug
      
      dataset = transform(dataset, **transform_options)
      
      if config.stats
        stats(dataset)
      elsif config.limit && !config.output
        # Just show first N rows
        peek(dataset, config.limit)
      else
        export(dataset, config.output, format: config.format)
      end
    end
    
    # Rest of the methods remain the same...
    # [check_mode, clear_mode, append_mode, write_mode, default_mode, col_to_index]
    
    def check_mode(config)
      sources = config.input_files
      sources << config.input if config.input
      
      issues = { blanks: 0, duplicates: 0 }
      
      sources.each do |source|
        dataset = read(source, sheet: config.sheet, range: config.range)
        
        blank_rows = dataset.each_with_index.count do |row|
          row.all? { |v| v.nil? || v.to_s.strip.empty? }
        end
        
        seen = Set.new
        duplicate_rows = dataset.each_with_index.count do |row|
          norm = row.map { |v| v.to_s.strip rescue v }
          if seen.include?(norm)
            true
          else
            seen.add(norm)
            false
          end
        end
        
        if blank_rows > 0 || duplicate_rows > 0
          puts "File: #{source}"
          puts "  Blank rows: #{blank_rows}"
          puts "  Duplicate rows: #{duplicate_rows}"
          puts
        end
        
        issues[:blanks] += blank_rows
        issues[:duplicates] += duplicate_rows
      end
      
      if issues.values.any?(&:positive?)
        puts "Total issues: #{issues[:blanks]} blank rows, #{issues[:duplicates]} duplicate rows"
        exit 1
      else
        puts "No issues found"
      end
    end
    
    def clear_mode(config)
      require 'rubyXL'
      
      workbook = RubyXL::Parser.parse(config.clear)
      sheet = config.sheet ? workbook[config.sheet] : workbook[0]
      
      if config.range && config.range =~ /^([A-Z]+)(\d+):([A-Z]+)(\d+)$/
        col_start = col_to_index($1)
        row_start = $2.to_i - 1
        col_end = col_to_index($3)
        row_end = $4.to_i - 1
        
        (row_start..row_end).each do |r|
          (col_start..col_end).each do |c|
            sheet.add_cell(r, c, nil)
          end
        end
      else
        sheet.each { |row| row&.cells&.each { |c| c&.change_contents(nil) } }
      end
      
      workbook.write(config.clear)
      puts "Cleared #{config.range || 'sheet'} in #{config.clear}"
    rescue => e
      warn "Clear failed: #{e.message}" if config.debug
      exit 1
    end
    
    def append_mode(config)
      require 'rubyXL'
      
      sources = config.input_files
      sources << config.input if config.input
      
      if sources.empty?
        warn "Error: No input file specified for append"
        exit 1
      end
      
      input = read(sources.first)
      return if input.empty?
      
      if File.exist?(config.append)
        workbook = RubyXL::Parser.parse(config.append)
      else
        workbook = RubyXL::Workbook.new
      end
      
      sheet = config.sheet ? workbook[config.sheet] || workbook.add_worksheet(config.sheet) : workbook[0]
      
      last_row = -1
      sheet.each_with_index do |row, i|
        last_row = i if row && row.cells.any? { |c| c && c.value }
      end
      
      input.each_with_index do |row, r|
        row.each_with_index do |cell, c|
          sheet.add_cell(last_row + r + 2, c, cell)
        end
      end
      
      workbook.write(config.append)
      puts "Appended #{input.size} rows to #{config.append}"
    rescue => e
      warn "Append failed: #{e.message}" if config.debug
      exit 1
    end
    
    def write_mode(config)
      require 'rubyXL'
      
      sources = config.input_files
      sources << config.input if config.input
      
      if sources.empty?
        warn "Error: No input file specified for write"
        exit 1
      end
      
      input = read(sources.first)
      return if input.empty?
      
      workbook = if File.exist?(config.write)
                   RubyXL::Parser.parse(config.write)
                 else
                   RubyXL::Workbook.new
                 end
      
      sheet = config.sheet ? workbook[config.sheet] || workbook.add_worksheet(config.sheet) : workbook[0]
      
      if config.range && config.range =~ /^([A-Z]+)(\d+):/
        col_start = col_to_index($1)
        row_start = $2.to_i - 1
      else
        col_start = 0
        row_start = 0
      end
      
      input.each_with_index do |row, r|
        row.each_with_index do |cell, c|
          sheet.add_cell(row_start + r, col_start + c, cell)
        end
      end
      
      workbook.write(config.write)
      puts "Wrote #{input.size} rows to #{config.write}"
    rescue => e
      warn "Write failed: #{e.message}" if config.debug
      exit 1
    end
    
    def default_mode(config)
      sources = config.input_files
      sources << config.input if config.input
      
      if sources.empty? && !$stdin.tty?
        dataset = read('-', sheet: config.sheet, range: config.range)
      elsif sources.one?
        dataset = read(sources.first, sheet: config.sheet, range: config.range)
      else
        show_help
        exit 1
      end
      
      export(dataset, config.output, format: config.format)
    end
    
    def col_to_index(col)
      col.upcase.each_char.inject(0) { |sum, c| sum * 26 + c.ord - 64 } - 1
    end
  end

  # Instance methods for CLI
  def run(args = ARGV)
    config = CCV.parse_args(args)
    CCV.instance_variable_set(:@config, config)
    CCV.send(:execute, config)
  end
end

# Interactive console DSL
class CCV::Console
  include CCV::DSL
  
  def initialize
    @current_dataset = CCV::Dataset.new
  end
  
  def self.run
    console = new
    puts "CCV Interactive Console #{CCV::VERSION}"
    puts "Type 'exit' or 'quit' to exit"
    puts "Example: from('data.csv').where('amount > 100').peek"
    
    loop do
      print "ccv> "
      input = gets&.chomp
      break unless input
      
      case input.downcase
      when 'exit', 'quit'
        break
      when 'help'
        puts "Commands: from, select, where, order, limit, stats, peek, dropna, rename, to_csv, to_json"
      else
        begin
          result = console.instance_eval(input)
          puts result unless result.nil? || result == console
        rescue => e
          puts "Error: #{e.message}"
          puts e.backtrace if console.instance_variable_get(:@current_dataset)&.respond_to?(:debug)
        end
      end
    end
  end
end

# Auto-install missing gems
BEGIN {
  def require_or_install(gem_name)
    require gem_name
  rescue LoadError
    warn "Installing #{gem_name}..."
    system("gem install #{gem_name} --quiet --no-document")
    Gem.refresh
    require gem_name
  end
}

# Main execution
if __FILE__ == $PROGRAM_NAME
  if ARGV.empty? && $stdin.tty? && ENV['CCV_CONSOLE']
    CCV::Console.run
  else
    require_or_install('duckdb')
    
    begin
      CCV.new.run
    rescue => e
      warn "ccv: #{e.message}"
      exit 1
    end
  end
end
