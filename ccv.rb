#!/usr/bin/env ruby
# frozen_string_literal: true
# author: JH
# update: 2026-Mar-1
# email:  joshuahamil7@runbox.com

# CCV - CSV/Excel/XML Converter and Processor
# With consistent column mapping: A, B, C, column0, column1, and original names all work

$DEBUG = false

begin
  require 'csv'
  require 'duckdb'
  require 'nokogiri'
  require 'zip'
  require 'fileutils'
rescue LoadError => e
  puts "Missing required gem: #{e.message}"
  puts "Please install: gem install duckdb nokogiri rubyzip"
  exit 1
end

require 'optparse'
require 'tempfile'
require 'stringio'
require 'set'
require 'json'
require 'time'

# ============================================================================
# Helper Methods
# ============================================================================

module Helpers
  module_function

  def col_to_index(col)
    return 0 if col.nil?
    return col if col.is_a?(Numeric)
    return 0 if col.empty?
    col.upcase.each_char.inject(0) { |sum, c| sum * 26 + c.ord - 64 } - 1
  end

  def index_to_col(idx)
    return 'A' if idx.nil? || idx < 0
    result = +''
    n = idx + 1
    while n > 0
      n -= 1
      result = (65 + (n % 26)).chr + result
      n = n / 26
    end
    result
  end

  def parse_cell_ref(ref)
    return nil unless ref =~ /^([A-Z]+)(\d+)$/
    { col: col_to_index($1), row: $2.to_i }
  end

  def parse_range(range)
    return nil unless range =~ /^([A-Z]+)(\d+):([A-Z]+)(\d+)$/
    {
      start_col: col_to_index($1),
      start_row: $2.to_i,
      end_col: col_to_index($3),
      end_row: $4.to_i
    }
  end

  def likely_headers?(row_values)
    return false if row_values.empty?
    row_values.all? { |v| v.is_a?(String) && v.match?(/^[A-Za-z]/) }
  end

  def format_number(val)
    return val if val.nil?
    if val.is_a?(Float) && val.to_i == val
      val.to_i.to_s
    else
      val.to_s
    end
  end
end

include Helpers

# ============================================================================
# Columnar IR - Pure Streaming with Column Mapping
# ============================================================================

class ColumnarStream
  attr_reader :columns, :types, :row_enum, :column_mapping
  
  def initialize(columns, types, row_enum)
    @columns = columns.freeze if columns
    @types = types.freeze if types
    @row_enum = row_enum
    
    # Create bidirectional mapping: original_name <-> column0 <-> letter
    @column_mapping = {}
    @reverse_mapping = {}
    
    if @columns
      @columns.each_with_index do |col_name, idx|
        # Map original name -> columnX
        @column_mapping[col_name] = "column#{idx}"
        @column_mapping[col_name.downcase] = "column#{idx}"
        
        # Map columnX -> original name
        @reverse_mapping["column#{idx}"] = col_name
        
        # Map letter -> columnX
        letter = index_to_col(idx)
        @column_mapping[letter] = "column#{idx}"
        @column_mapping[letter.downcase] = "column#{idx}"
        
        # Map columnX -> letter
        @reverse_mapping["column#{idx}_letter"] = letter
        
        # Map index -> everything
        @column_mapping[idx] = "column#{idx}"
      end
    end
  end
  
  def get_column_ref(name)
    return "column#{name}" if name.is_a?(Integer)
    return @column_mapping[name] if @column_mapping&.key?(name)
    return @column_mapping[name.to_s] if @column_mapping&.key?(name.to_s)
    
    # Try as column letter
    if name.is_a?(String) && name.match?(/^[A-Z]+$/i)
      col_idx = col_to_index(name.upcase)
      return "column#{col_idx}" if col_idx < @columns&.size.to_i
    end
    
    name.to_s
  end
  
  def get_original_name(column_ref)
    return @reverse_mapping[column_ref] if @reverse_mapping&.key?(column_ref)
    
    if column_ref =~ /^column(\d+)$/
      idx = $1.to_i
      return @columns[idx] if @columns && idx < @columns.size
    end
    
    column_ref
  end
  
  # Translate SQL with any column references (names, letters, columnX) to columnX for DuckDB
  def translate_sql(sql)
    return sql unless @columns
    
    translated = sql.dup
    
    # Replace original column names with columnX
    @columns.each_with_index do |col_name, idx|
      # Use word boundaries to avoid partial matches
      translated.gsub!(/\b#{Regexp.escape(col_name)}\b/, "column#{idx}")
    end
    
    # Replace column letters (A, B, C) with columnX
    @columns.each_with_index do |_, idx|
      letter = index_to_col(idx)
      translated.gsub!(/\b#{letter}\b/, "column#{idx}")
    end
    
    # columnX is already correct, leave as is
    
    translated
  end

  def filter(&predicate)
    new_enum = Enumerator.new do |yielder|
      @row_enum.each do |row|
        begin
          context = RowContext.new(@columns, @types, row, @column_mapping, @reverse_mapping)
          yielder << row if predicate.call(context)
        rescue => e
          warn "Filter error: #{e.message}" if $DEBUG
        end
      end
    end
    ColumnarStream.new(@columns, @types, new_enum)
  end

  def select(*keep_columns)
    keep_columns = keep_columns.map(&:to_s)
    
    if @columns
      # Convert any column reference to index
      indices = keep_columns.map do |col|
        # Try as columnX
        if col =~ /^column(\d+)$/i
          $1.to_i
        # Try as letter
        elsif col.match?(/^[A-Z]+$/i)
          col_to_index(col.upcase)
        # Try as original name
        else
          @columns.index(col) || @columns.index { |c| c.downcase == col.downcase }
        end
      end.compact
      
      new_columns = indices.map { |i| @columns[i] }
      new_types = @types ? indices.map { |i| @types[i] } : nil
    else
      indices = keep_columns.map { |c| col_to_index(c) }
      new_columns = keep_columns
      new_types = nil
    end
    
    new_enum = Enumerator.new do |yielder|
      @row_enum.each do |row|
        row_array = row.is_a?(Array) ? row : [row]
        selected = indices.map do |i|
          i && i < row_array.size ? row_array[i] : nil
        end
        yielder << selected
      end
    end
    
    ColumnarStream.new(new_columns, new_types, new_enum)
  end

  def limit(n)
    count = 0
    new_enum = Enumerator.new do |yielder|
      @row_enum.each do |row|
        if count < n
          yielder << row
          count += 1
        else
          break
        end
      end
    end
    ColumnarStream.new(@columns, @types, new_enum)
  end

  def map(&block)
    new_enum = Enumerator.new do |yielder|
      @row_enum.each do |row|
        begin
          context = RowContext.new(@columns, @types, row, @column_mapping, @reverse_mapping)
          yielder << block.call(context)
        rescue => e
          warn "Map error: #{e.message}" if $DEBUG
        end
      end
    end
    ColumnarStream.new(nil, nil, new_enum)
  end

  def to_a
    @row_enum.to_a
  end
  
  def count
    @row_enum.count
  end
  
  def first(n = nil)
    n ? @row_enum.first(n) : @row_enum.first
  end
  
  def each(&block)
    @row_enum.each(&block)
  end
end

class RowContext
  def initialize(columns, types, row, column_mapping, reverse_mapping)
    @columns = columns
    @types = types
    @row = row
    @column_mapping = column_mapping
    @reverse_mapping = reverse_mapping
    @column_indices = {}
    
    if @columns
      @columns.each_with_index do |col_name, idx|
        @column_indices[col_name] = idx
        @column_indices[col_name.downcase] = idx
        @column_indices["column#{idx}"] = idx
        @column_indices[idx] = idx
        @column_indices[index_to_col(idx)] = idx
        @column_indices[index_to_col(idx).downcase] = idx
      end
    end
  end

  def method_missing(name, *args)
    return super if @columns.nil?
    
    name_str = name.to_s
    idx = @column_indices[name_str]
    
    # Try as column letter
    if idx.nil? && name_str.match?(/^[A-Z]+$/i)
      col_idx = col_to_index(name_str.upcase)
      idx = col_idx if col_idx < @columns.size
    end
    
    return nil unless idx
    
    val = @row[idx]
    
    if @types && idx < @types.size
      case @types[idx]
      when :integer
        val.to_i rescue val
      when :float
        val.to_f rescue val
      else
        val.to_s
      end
    else
      val
    end
  end

  def [](key)
    return nil if @columns.nil?
    
    idx = if key.is_a?(Integer)
      key
    elsif key.is_a?(String) && key =~ /^column(\d+)$/i
      $1.to_i
    else
      @column_indices[key.to_s]
    end
    
    # Try as column letter
    if idx.nil? && key.to_s.match?(/^[A-Z]+$/i)
      idx = col_to_index(key.to_s.upcase)
    end
    
    return nil unless idx && idx < @row.size
    
    val = @row[idx]
    
    if @types && idx < @types.size
      case @types[idx]
      when :integer
        val.to_i rescue val
      when :float
        val.to_f rescue val
      else
        val.to_s
      end
    else
      val
    end
  end

  def respond_to_missing?(name, include_private = false)
    name_str = name.to_s
    return true if @column_indices.key?(name_str)
    return true if name_str.match?(/^[A-Z]+$/i) && col_to_index(name_str.upcase) < @columns&.size.to_i
    super
  end
end

# ============================================================================
# Readers - Pure Streaming
# ============================================================================

module Reader
  def self.detect_type(value)
    return :string if value.nil? || value.empty?
    s = value.to_s
    return :integer if s.match?(/^-?\d+$/)
    return :float if s.match?(/^-?\d*\.\d+$/)
    :string
  end

  def self.cast(value, type)
    return value if value.nil?
    case type
    when :integer then value.to_i
    when :float then value.to_f
    else value.to_s
    end
  end

  def self.csv(path, headers: true)
    return stdin_csv(headers: headers) if path == '-'

    columns = nil
    types = nil
    first_data_row = nil
    header_skipped = false
    
    # Peek at first two rows to determine structure
    CSV.open(path, 'r') do |csv|
      if headers
        header_row = csv.shift
        columns = header_row.map(&:to_s) if header_row
      end
      first_data_row = csv.shift
      if first_data_row
        types = first_data_row.map { |v| detect_type(v) }
      end
    end

    row_enum = Enumerator.new do |yielder|
      row_num = 0
      
      CSV.foreach(path, headers: false) do |row|
        if headers && !header_skipped
          header_skipped = true
          next
        end
        
        if types
          typed_row = row.each_with_index.map { |val, i| cast(val, types[i]) }
          yielder << typed_row
        else
          yielder << row
        end
        row_num += 1
      end
    end

    ColumnarStream.new(columns, types, row_enum)
  end

  def self.stdin_csv(headers: true)
    columns = nil
    types = nil
    first_row_processed = false
    header_skipped = false
    
    row_enum = Enumerator.new do |yielder|
      $stdin.each_line do |line|
        line = line.chomp
        next if line.empty?
        row = CSV.parse_line(line)
        next if row.nil?
        
        unless first_row_processed
          first_row_processed = true
          if headers
            columns = row.map(&:to_s)
            header_skipped = true
            next
          else
            types = row.map { |v| detect_type(v) }
            columns = (0...row.size).map { |i| index_to_col(i) }
          end
        end
        
        if types && !header_skipped
          typed_row = row.each_with_index.map { |val, i| cast(val, types[i]) }
          yielder << typed_row
        elsif !header_skipped
          yielder << row
        end
      end
    end

    ColumnarStream.new(columns, types, row_enum)
  end

  def self.xlsx(path, sheet: nil, range: nil, headers: true)
    range_info = range ? parse_range(range) : nil
    
    start_row = range_info ? range_info[:start_row] : nil
    end_row = range_info ? range_info[:end_row] : nil
    start_col_idx = range_info ? range_info[:start_col] : nil
    end_col_idx = range_info ? range_info[:end_col] : nil
    
    column_count = range_info ? (end_col_idx - start_col_idx + 1) : nil
    
    columns = if range_info
      (0...column_count).map { |i| index_to_col(i) }
    else
      nil
    end
    
    row_enum = Enumerator.new do |yielder|
      rows_in_range = 0
      header_skipped = false
      
      parser = XLSXStreamer.new(path, sheet) do |row_data, row_num|
        next if row_data.nil? || row_data.empty?
        
        if range_info
          next if row_num < start_row || row_num > end_row
        end
        
        if range_info
          filtered_row = {}
          row_data.each do |col_letter, val|
            col_idx = col_to_index(col_letter)
            if col_idx >= start_col_idx && col_idx <= end_col_idx
              new_col = index_to_col(col_idx - start_col_idx)
              filtered_row[new_col] = val
            end
          end
          row_data = filtered_row
        end
        
        next if row_data.nil? || row_data.empty?
        
        if !range_info && columns.nil?
          columns = row_data.keys.sort
        end
        
        if range_info
          typed_row = columns.map { |c| row_data[c].to_s }
          yielder << typed_row
          rows_in_range += 1
        elsif headers
          if rows_in_range == 0 && likely_headers?(row_data.values)
            header_skipped = true
          else
            typed_row = columns.map { |c| row_data[c].to_s }
            yielder << typed_row
            rows_in_range += 1
          end
        else
          typed_row = columns.map { |c| row_data[c].to_s }
          yielder << typed_row
          rows_in_range += 1
        end
      end
      parser.parse
    end

    ColumnarStream.new(columns, nil, row_enum)
  end

  def self.xml(path, record_path: '//row', headers: true)
    columns = nil
    first_row = true
    header_skipped = false
    
    row_enum = Enumerator.new do |yielder|
      parser = XMLStreamer.new(path, record_path) do |row_data|
        if first_row
          columns = row_data.keys.sort
          first_row = false
          
          if headers && likely_headers?(row_data.values)
            header_skipped = true
            next
          end
        end
        
        unless header_skipped
          typed_row = columns.map { |c| row_data[c].to_s }
          yielder << typed_row
        end
      end
      parser.parse
    end

    ColumnarStream.new(columns, nil, row_enum)
  end

  def self.auto(path, **options)
    headers = options.fetch(:headers, true)
    sheet = options[:sheet]
    range = options[:range]
    record_path = options.fetch(:record_path, '//row')
    
    case path
    when /\.csv$/i, '-'
      csv(path, headers: headers)
    when /\.xlsx?$/i
      xlsx(path, headers: headers, sheet: sheet, range: range)
    when /\.xml$/i
      xml(path, headers: headers, record_path: record_path)
    else
      csv(path, headers: headers)
    end
  end
end

# ============================================================================
# XLSX Streamer - Pure Streaming SAX Parser
# ============================================================================

class XLSXStreamer < Nokogiri::XML::SAX::Document
  include Helpers

  def initialize(path, target_sheet = nil, &block)
    @path = path
    @target_sheet = target_sheet
    @block = block
    @current_row = {}
    @shared_strings = []
    @in_sheet = false
    @row_num = 0
    @col_num = 0
    @in_value = false
    @value = +''
    @current_cell = nil
  end

  def parse
    parse_shared_strings
    
    Zip::File.open(@path) do |zip|
      sheet_names = get_sheet_names(zip)
      
      zip.each do |entry|
        if entry.name =~ /xl\/worksheets\/sheet(\d+)\.xml$/
          sheet_num = $1
          sheet_name = sheet_names[sheet_num] || "Sheet#{sheet_num}"
          
          if @target_sheet.nil? || sheet_name == @target_sheet || "Sheet#{sheet_num}" == @target_sheet
            @in_sheet = true
            parser = Nokogiri::XML::SAX::Parser.new(self)
            parser.parse(entry.get_input_stream.read)
            @in_sheet = false
          end
        end
      end
    end
  end

  def start_element(name, attrs = [])
    return unless @in_sheet

    case name
    when 'row'
      @current_row = {}
      attrs_hash = Hash[attrs]
      @row_num = attrs_hash['r'].to_i if attrs_hash['r']
      @col_num = 0
    when 'c'
      attrs_hash = Hash[attrs]
      @current_cell = { type: attrs_hash['t'] }
      if attrs_hash['r'] && attrs_hash['r'] =~ /^([A-Z]+)/
        @col_num = col_to_index($1)
      end
    when 'v', 'is', 't'
      @in_value = true
      @value = +''
    end
  end

  def characters(text)
    @value << text if @in_value
  end

  def end_element(name)
    return unless @in_sheet

    case name
    when 'v', 't', 'is'
      @in_value = false
      if @current_cell && @current_cell[:type] == 's'
        idx = @value.to_i
        @current_row[@col_num] = @shared_strings[idx] if @shared_strings[idx]
      else
        @current_row[@col_num] = @value
      end
      
    when 'row'
      row_hash = {}
      @current_row.each do |col_idx, val|
        row_hash[index_to_col(col_idx)] = val
      end
      @block.call(row_hash, @row_num) unless row_hash.empty?
      @current_row = {}
    end
  end

  private

  def get_sheet_names(zip)
    sheet_names = {}
    workbook_entry = zip.find { |e| e.name =~ /xl\/workbook\.xml$/ }
    
    if workbook_entry
      content = workbook_entry.get_input_stream.read
      doc = Nokogiri::XML(content)
      doc.remove_namespaces!
      doc.xpath('//sheets/sheet').each do |sheet|
        sheet_id = sheet['sheetId']
        sheet_name = sheet['name']
        sheet_names[sheet_id] = sheet_name if sheet_id && sheet_name
      end
    end
    
    sheet_names
  end

  def parse_shared_strings
    Zip::File.open(@path) do |zip|
      entry = zip.find { |e| e.name =~ /xl\/sharedStrings\.xml$/ }
      return unless entry
      
      parser = Nokogiri::XML::SAX::Parser.new(SharedStringHandler.new(@shared_strings))
      parser.parse(entry.get_input_stream.read)
    end
  end

  class SharedStringHandler < Nokogiri::XML::SAX::Document
    def initialize(store)
      @store = store
      @in_t = false
      @current = +''
    end

    def start_element(name, _)
      @in_t = (name == 't')
      @current = +'' if name == 'si'
    end

    def characters(text)
      @current << text if @in_t
    end

    def end_element(name)
      @store << @current if name == 'si'
    end
  end
end

# ============================================================================
# XML Streamer - Pure Streaming SAX Parser
# ============================================================================

class XMLStreamer < Nokogiri::XML::SAX::Document
  include Helpers

  def initialize(path, record_path, &block)
    @path = path
    @record_path = record_path
    @block = block
    @current_record = {}
    @current_element = nil
    @depth = 0
    @in_record = false
    @target_depth = record_path.split('/').reject(&:empty?).size
    @current_text = +''
  end

  def parse
    parser = Nokogiri::XML::SAX::Parser.new(self)
    if @path == '-'
      parser.parse($stdin)
    else
      File.open(@path) { |f| parser.parse(f) }
    end
  end

  def start_element(name, attrs = [])
    @depth += 1
    
    if @depth == @target_depth
      @in_record = true
      @current_record = {}
    end
    
    @current_element = name if @in_record
    @current_text = +''
  end

  def characters(text)
    return unless @in_record && @current_element
    @current_text << text
  end

  def end_element(name)
    if @in_record && @current_element == name
      @current_record[@current_element] = @current_text.strip unless @current_text.strip.empty?
    end
    
    if @in_record && @depth == @target_depth
      @block.call(@current_record)
      @in_record = false
    end
    @depth -= 1
    @current_element = nil
  end
end

# ============================================================================
# Transformations - Streaming
# ============================================================================

module Transform
  module_function
  include Helpers

  def select(stream, *columns)
    stream.select(*columns)
  end

  def filter(stream, &predicate)
    stream.filter(&predicate)
  end

  def filter_with_columns(stream, condition)
    stream.filter do |context|
      begin
        eval_condition(context, condition)
      rescue => e
        warn "Filter error: #{e.message}" if $DEBUG
        false
      end
    end
  end

  def eval_condition(context, condition)
    context.instance_eval(condition)
  rescue => e
    warn "Filter syntax error: #{e.message}" if $DEBUG
    false
  end

  def limit(stream, n)
    stream.limit(n)
  end
end

# ============================================================================
# Writers - Streaming output
# ============================================================================

module Writer
  include Helpers

  def self.csv(stream, path = nil, headers: true)
    io = path && path != '-' ? File.open(path, 'w') : $stdout

    begin
      if headers && stream.columns
        io.puts(stream.columns.join(','))
      end
      
      stream.each do |row|
        formatted_row = row.map { |v| v.is_a?(Numeric) ? format_number(v) : v }
        io.puts(formatted_row.join(','))
      end
    ensure
      io.close if io != $stdout
    end
  end

  def self.xlsx(stream, path = nil, sheet: 'Sheet1', start_cell: 'A1', headers: true, inplace: true, mode: 'append')
    raise "XLSX output requires a file path" if path.nil? || path == '-'

    start_col, start_row = parse_start_cell(start_cell)
    
    if File.exist?(path) && inplace
      update_existing_xlsx(stream, path, sheet, start_col, start_row, headers, mode)
    else
      create_new_xlsx(stream, path, sheet, start_col, start_row, headers)
    end
  end

  def self.create_new_xlsx(stream, path, sheet_name, start_col, start_row, headers)
    original_path = File.absolute_path(path)
    temp_dir = File.dirname(original_path)
    temp_file = File.join(temp_dir, ".tmp_#{Time.now.to_i}_#{rand(100000)}.xlsx")
    
    begin
      Zip::OutputStream.open(temp_file) do |zos|
        write_content_types(zos)
        write_rels(zos)
        write_workbook_rels(zos)
        write_workbook(zos, sheet_name)
        
        zos.put_next_entry('xl/worksheets/sheet1.xml')
        zos.write <<~XML
          <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
          <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
            <sheetData>
        XML
        
        row_count = 0
        first_row = nil
        
        stream.each do |row|
          first_row ||= row
          zos.write row_to_xml(start_row + row_count, start_col, row)
          row_count += 1
        end
        
        last_row = start_row + row_count - 1
        last_col = start_col + (first_row&.size || 1) - 1
        
        zos.write <<~XML
            </sheetData>
            <dimension ref="#{index_to_col(start_col)}#{start_row}:#{index_to_col(last_col)}#{last_row}"/>
          </worksheet>
        XML
      end

      File.rename(temp_file, original_path)
      
    rescue => e
      FileUtils.rm_f(temp_file) if File.exist?(temp_file)
      raise
    end
  end

  def self.update_existing_xlsx(stream, path, sheet_name, start_col, start_row, headers, mode = 'append')
    original_path = File.absolute_path(path)
    temp_dir = File.dirname(original_path)
    temp_file = File.join(temp_dir, ".tmp_#{Time.now.to_i}_#{rand(100000)}.xlsx")
    
    begin
      # Read existing entries
      existing_entries = {}
      Zip::File.open(original_path) do |zip|
        zip.each do |entry|
          next if entry.name.start_with?('__MACOSX/')
          existing_entries[entry.name] = entry.get_input_stream.read
        end
      end

      # Find or create sheet
      sheet_path, sheet_doc, sheet_data = find_or_create_sheet(existing_entries, sheet_name)
      
      # Determine start row for append mode
      if mode.downcase == 'append'
        max_row = start_row - 1
        sheet_data.xpath('.//*[local-name()="row"]').each do |row_elem|
          row_num = row_elem['r'].to_i
          max_row = row_num if row_num > max_row
        end
        current_row = max_row + 1
      else
        current_row = start_row
      end
      
      # Stream rows directly into XML
      row_idx = 0
      stream.each do |row|
        target_row = current_row + row_idx
        
        row_elem = Nokogiri::XML::Node.new('row', sheet_doc)
        row_elem['r'] = target_row.to_s
        
        row.each_with_index do |val, col_idx|
          target_col = start_col + col_idx
          col_letter = index_to_col(target_col)
          cell_ref = "#{col_letter}#{target_row}"
          
          cell = Nokogiri::XML::Node.new('c', sheet_doc)
          cell['r'] = cell_ref
          
          if val.is_a?(Numeric)
            cell['t'] = 'n'
            v = Nokogiri::XML::Node.new('v', sheet_doc)
            v.content = val.to_s
            cell.add_child(v)
          else
            cell['t'] = 'inlineStr'
            is = Nokogiri::XML::Node.new('is', sheet_doc)
            t = Nokogiri::XML::Node.new('t', sheet_doc)
            t.content = val.to_s
            is.add_child(t)
            cell.add_child(is)
          end
          
          row_elem.add_child(cell)
        end
        
        sheet_data.add_child(row_elem)
        row_idx += 1
      end
      
      # Update dimension
      update_dimension(sheet_doc)
      
      # Save updated sheet
      existing_entries[sheet_path] = sheet_doc.to_xml
      
      # Write new ZIP
      Zip::OutputStream.open(temp_file) do |zos|
        # Ensure [Content_Types].xml is first
        if existing_entries['[Content_Types].xml']
          zos.put_next_entry('[Content_Types].xml')
          zos.write(existing_entries['[Content_Types].xml'])
          existing_entries.delete('[Content_Types].xml')
        end
        
        existing_entries.each do |name, content|
          zos.put_next_entry(name)
          zos.write(content)
        end
      end
      
      File.rename(temp_file, original_path)
      
    rescue => e
      FileUtils.rm_f(temp_file) if File.exist?(temp_file)
      raise
    end
  end

  def self.jsonl(stream, path = nil, headers: true)
    io = path && path != '-' ? File.open(path, 'w') : $stdout

    begin
      stream.each do |row|
        if headers && stream.columns
          output = {}
          stream.columns.each_with_index { |col, i| output[col] = row[i] if i < row.size }
          io.puts(output.to_json)
        else
          output = {}
          row.each_with_index { |val, i| output[index_to_col(i)] = val }
          io.puts(output.to_json)
        end
      end
    ensure
      io.close if io != $stdout
    end
  end

  def self.auto(stream, path = nil, **options)
    headers = options.fetch(:headers, true)
    mode = options.fetch(:mode, 'append')
    
    case path
    when /\.xlsx?$/i
      inplace = options.fetch(:inplace, true)
      sheet = options[:sheet] || 'Sheet1'
      start_cell = options[:start_cell] || 'A1'
      xlsx(stream, path, sheet: sheet, start_cell: start_cell, headers: headers, inplace: inplace, mode: mode)
    when /\.jsonl?$/i
      jsonl(stream, path, headers: headers)
    else
      csv(stream, path, headers: headers)
    end
  end

  def self.row_to_xml(row_num, start_col, values)
    xml = %(<row r="#{row_num}">)
    values.each_with_index do |val, i|
      next if val.nil?
      col = index_to_col(start_col + i)
      if val.is_a?(Numeric)
        xml << %(<c r="#{col}#{row_num}" t="n"><v>#{val}</v></c>)
      else
        escaped = val.to_s.gsub('&', '&amp;').gsub('<', '&lt;').gsub('>', '&gt;')
        xml << %(<c r="#{col}#{row_num}" t="inlineStr"><is><t>#{escaped}</t></is></c>)
      end
    end
    xml << "</row>"
  end

  def self.parse_start_cell(cell)
    if cell =~ /^([A-Z]+)(\d+)$/
      [col_to_index($1), $2.to_i]
    else
      [0, 1]
    end
  end

  def self.write_content_types(zos)
    zos.put_next_entry('[Content_Types].xml')
    zos.write <<~XML
      <?xml version="1.0" encoding="UTF-8"?>
      <Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
        <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
        <Default Extension="xml" ContentType="application/xml"/>
        <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
        <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
      </Types>
    XML
  end

  def self.write_rels(zos)
    zos.put_next_entry('_rels/.rels')
    zos.write <<~XML
      <?xml version="1.0" encoding="UTF-8"?>
      <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
        <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
      </Relationships>
    XML
  end

  def self.write_workbook_rels(zos)
    zos.put_next_entry('xl/_rels/workbook.xml.rels')
    zos.write <<~XML
      <?xml version="1.0" encoding="UTF-8"?>
      <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
        <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
      </Relationships>
    XML
  end

  def self.write_workbook(zos, sheet_name)
    zos.put_next_entry('xl/workbook.xml')
    zos.write <<~XML
      <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
      <workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" 
                xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
        <sheets>
          <sheet name="#{sheet_name}" sheetId="1" r:id="rId1"/>
        </sheets>
      </workbook>
    XML
  end

  def self.find_or_create_sheet(entries, sheet_name)
    workbook_doc = Nokogiri::XML(entries['xl/workbook.xml'])
    sheet = workbook_doc.at_xpath("//*[local-name()='sheet'][@name='#{sheet_name}']")
    
    if sheet
      sheet_id = sheet['sheetId']
      sheet_path = "xl/worksheets/sheet#{sheet_id}.xml"
      sheet_doc = Nokogiri::XML(entries[sheet_path])
    else
      # Create new sheet
      max_id = entries.keys.grep(/sheet(\d+)\.xml$/).map { |k| $1.to_i }.max || 0
      sheet_id = max_id + 1
      sheet_path = "xl/worksheets/sheet#{sheet_id}.xml"
      
      sheet_doc = Nokogiri::XML(<<~XML)
        <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
          <sheetData>
          </sheetData>
        </worksheet>
      XML
      
      # Add to workbook
      sheets = workbook_doc.at_xpath('//*[local-name()="sheets"]')
      new_sheet = Nokogiri::XML::Node.new('sheet', workbook_doc)
      new_sheet['name'] = sheet_name
      new_sheet['sheetId'] = sheet_id.to_s
      new_sheet['r:id'] = "rId#{sheet_id}"
      sheets.add_child(new_sheet)
      entries['xl/workbook.xml'] = workbook_doc.to_xml
      
      # Add relationship
      rels_path = 'xl/_rels/workbook.xml.rels'
      if entries[rels_path]
        rels_doc = Nokogiri::XML(entries[rels_path])
      else
        rels_doc = Nokogiri::XML(<<~XML)
          <?xml version="1.0" encoding="UTF-8"?>
          <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
          </Relationships>
        XML
      end
      
      rel = Nokogiri::XML::Node.new('Relationship', rels_doc)
      rel['Id'] = "rId#{sheet_id}"
      rel['Type'] = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet'
      rel['Target'] = "worksheets/sheet#{sheet_id}.xml"
      rels_doc.root.add_child(rel)
      entries[rels_path] = rels_doc.to_xml
    end
    
    sheet_data = sheet_doc.at_xpath('//*[local-name()="sheetData"]')
    unless sheet_data
      sheet_data = Nokogiri::XML::Node.new('sheetData', sheet_doc)
      sheet_doc.root.add_child(sheet_data)
    end
    
    [sheet_path, sheet_doc, sheet_data]
  end

  def self.update_dimension(sheet_doc)
    rows = sheet_doc.xpath('//*[local-name()="row"]')
    return if rows.empty?
    
    min_row = rows.map { |r| r['r'].to_i }.min
    max_row = rows.map { |r| r['r'].to_i }.max
    
    cells = sheet_doc.xpath('//*[local-name()="c"]')
    min_col = cells.map { |c| col_to_index(c['r'][/^[A-Z]+/]) }.min || 0
    max_col = cells.map { |c| col_to_index(c['r'][/^[A-Z]+/]) }.max || 0
    
    dim_ref = "#{index_to_col(min_col)}#{min_row}:#{index_to_col(max_col)}#{max_row}"
    
    dim = sheet_doc.at_xpath('//*[local-name()="dimension"]')
    if dim
      dim['ref'] = dim_ref
    else
      dim = Nokogiri::XML::Node.new('dimension', sheet_doc)
      dim['ref'] = dim_ref
      sheet_doc.root.prepend_child(dim)
    end
  end
end

# ============================================================================
# DuckDB Engine - With Column Mapping
# ============================================================================

module DuckDBEngine
  module_function
  include Helpers

  def self.query(stream, sql)
    return enum_for(:query, stream, sql) unless block_given?

    Tempfile.create(['ccv', '.csv']) do |temp|
      # Write data with column0, column1 headers for DuckDB
      CSV.open(temp.path, 'w') do |csv|
        if stream.columns
          # Use column0, column1... as headers for DuckDB
          headers = stream.columns.size.times.map { |i| "column#{i}" }
          csv << headers
        end
        stream.each do |row|
          csv << row
        end
      end

      DuckDB::Database.open do |db|
        db.connect do |conn|
          # Translate SQL to use column0, column1...
          # This handles original names, letters, and columnX references
          translated_sql = stream.translate_sql(sql)
          puts "Translated SQL: #{translated_sql}" if $DEBUG
          
          begin
            conn.execute("CREATE VIEW data AS SELECT * FROM read_csv_auto('#{temp.path}', HEADER=true)")
            result = conn.execute(translated_sql)
            result_columns = result.columns.map(&:name)
            
            result_rows = Enumerator.new do |y|
              result.each { |row| y << row.to_a }
            end
            
            yield ColumnarStream.new(result_columns, nil, result_rows)
          rescue => e
            # Fallback: try without headers
            conn.execute("CREATE VIEW data AS SELECT * FROM read_csv_auto('#{temp.path}', HEADER=false)")
            result = conn.execute(translated_sql)
            result_columns = result.columns.map(&:name)
            
            result_rows = Enumerator.new do |y|
              result.each { |row| y << row.to_a }
            end
            
            yield ColumnarStream.new(result_columns, nil, result_rows)
          end
        end
      end
    end
  end
end

# ============================================================================
# Configuration and Pipeline
# ============================================================================

Config = Struct.new(
  :input, :output, :query, :operations, :format, 
  :sheet, :range, :start_cell, :debug, :gzip, :clear_range, 
  :headers, :header_row, :skip_rows, :mode, :record_path, keyword_init: true
) do
  def self.from_args(args)
    new(operations: [], headers: true, header_row: 0, skip_rows: 0, mode: 'append', record_path: '//row').tap { |c| Parser.parse(args, c) }
  end

  def input_stream?
    input == '-' || input.nil?
  end

  def output_stream?
    output.nil? || output == '-'
  end
end

module Parser
  module_function

  def parse(args, config)
    i = 0
    while i < args.length
      case args[i]
      when 'from'
        config.input = args[i + 1]
        i += 2
        while i < args.length && %w[sheet range record_path].include?(args[i])
          case args[i]
          when 'sheet' then config.sheet = args[i + 1]
          when 'range' then config.range = args[i + 1]
          when 'record_path' then config.record_path = args[i + 1]
          end
          i += 2
        end
      when 'to'
        config.output = args[i + 1]
        i += 2
        while i < args.length && %w[sheet start mode].include?(args[i])
          case args[i]
          when 'sheet' then config.sheet = args[i + 1]
          when 'start' then config.start_cell = args[i + 1]
          when 'mode' then config.mode = args[i + 1]
          end
          i += 2
        end
      when '--no-headers'
        config.headers = false
        i += 1
      when 'select'
        cols = []
        i += 1
        while i < args.length && !args[i].start_with?('-') && !%w[from to where filter limit].include?(args[i])
          cols << args[i]
          i += 1
        end
        config.operations << { type: :select, args: cols }
      when 'filter', 'where'
        config.operations << { type: :filter, args: [args[i + 1]] }
        i += 2
      when 'limit'
        config.operations << { type: :limit, args: [args[i + 1].to_i] }
        i += 2
      when '-q', '--query'
        config.query = args[i + 1]
        i += 2
      when '--debug'
        config.debug = true
        $DEBUG = true
        i += 1
      else
        config.input = args[i] if config.input.nil?
        i += 1
      end
    end
  end
end

class Pipeline
  attr_reader :config

  def initialize(config)
    @config = config
  end

  def run
    stream = read_input
    
    if config.operations.any?
      stream = apply_transforms(stream)
    end
    
    if config.query
      result_stream = nil
      DuckDBEngine.query(stream, config.query) { |s| result_stream = s }
      stream = result_stream if result_stream
    end
    
    write_output(stream)
  end

  private

  def read_input
    if config.input_stream?
      Reader.auto('-', headers: config.headers)
    elsif File.exist?(config.input)
      if File.size(config.input) == 0
        warn "Warning: Input file is empty"
        return ColumnarStream.new(nil, nil, [].each)
      end
      
      options = { headers: config.headers }
      options[:sheet] = config.sheet if config.sheet
      options[:range] = config.range if config.range
      options[:record_path] = config.record_path if config.record_path
      
      Reader.auto(config.input, **options)
    else
      warn "Error: Input file '#{config.input}' does not exist"
      exit 1
    end
  end

  def apply_transforms(stream)
    config.operations.reduce(stream) do |s, op|
      case op[:type]
      when :select
        s = Transform.select(s, *op[:args])
      when :filter, :where
        s = Transform.filter_with_columns(s, op[:args].first)
      when :limit
        s = Transform.limit(s, op[:args].first)
      else
        s
      end
      s
    end
  end

  def write_output(stream)
    output_path = if config.output_stream?
      '-'
    elsif config.output
      config.output
    else
      '-'
    end
    
    options = { headers: config.headers, mode: config.mode }
    
    case output_path
    when /\.xlsx?$/i
      options[:sheet] = config.sheet if config.sheet
      options[:start_cell] = config.start_cell if config.start_cell
    end
    
    Writer.auto(stream, output_path, **options)
  end
end

module CCV
  module_function

  def run(args = ARGV)
    config = Config.from_args(args)
    Pipeline.new(config).run
  rescue => e
    warn "Error: #{e.message}"
    warn e.backtrace if config&.debug
    exit 1
  end
end

CCV.run if __FILE__ == $PROGRAM_NAME
