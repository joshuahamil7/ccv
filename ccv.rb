#!/usr/bin/env ruby
# frozen_string_literal: true
# author: JH
# update: 2026-Mar-5
# email: joshuahamil7@runbox.com

# CCV - CSV/Excel/XML Converter and Processor


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
require 'timeout'

# ============================================================================
# Helper Methods
# ============================================================================

module Helpers
  module_function

  # Convert Excel column letter(s) to zero-based index
  def col_to_index(col)
    return 0 if col.nil?
    return col if col.is_a?(Numeric)
    return 0 if col.empty?
    col.to_s.upcase.each_char.inject(0) { |sum, c| sum * 26 + c.ord - 64 } - 1
  end

  # Convert zero-based index to Excel column letter(s)
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

  # Convert index to columnX name
  def index_to_columnX(idx)
    "column#{idx}"
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
    return false if row_values.nil? || row_values.empty?
    return false if row_values.any? { |v| v.to_s.match?(/^-?\d+(\.\d+)?$/) }
    row_values.all? { |v| v.is_a?(String) && v.to_s.match?(/^[A-Za-z]/) }
  rescue
    false
  end

  def format_number(val)
    return '' if val.nil?
    if val.is_a?(Float) && val.to_i == val
      val.to_i.to_s
    elsif val.is_a?(Numeric)
      val.to_s
    else
      val.to_s
    end
  end

  def escape_xml(str)
    str.to_s.gsub('&', '&amp;')
           .gsub('<', '&lt;')
           .gsub('>', '&gt;')
           .gsub('"', '&quot;')
           .gsub("'", '&apos;')
  end
end

include Helpers

# ============================================================================
# CCV Config Registry - Central KV store for pipeline configuration
# ============================================================================

class CCVConfig
  attr_reader :store, :metadata, :transformations, :column_mapping

  def initialize
    @store = {}
    @metadata = {}
    @transformations = []
    @column_mapping = {}
  end

  def set(key, value)
    @store[key.to_sym] = value
  end

  def get(key, default = nil)
    @store.fetch(key.to_sym, default)
  end

  def set_metadata(source, data)
    @metadata[source.to_sym] = data
  end

  def get_metadata(source)
    @metadata[source.to_sym]
  end

  def add_transform(type, *args)
    @transformations << { type: type, args: args }
  end

  def transformations
    @transformations
  end

  def build_column_mapping!
    cols = get_metadata(:source)&.dig(:columns) || []
    @column_mapping = {}
    
    cols.each_with_index do |col_name, idx|
      @column_mapping[col_name] = idx
      @column_mapping[col_name.downcase] = idx
      @column_mapping[index_to_col(idx)] = idx
      @column_mapping[index_to_col(idx).downcase] = idx
      @column_mapping["column#{idx}"] = idx
    end
  end

  def column_index(ref)
    return ref if ref.is_a?(Integer)
    return @column_mapping[ref] if @column_mapping.key?(ref)
    return @column_mapping[ref.to_s] if @column_mapping.key?(ref.to_s)
    
    if ref.is_a?(String) && ref.match?(/^[A-Z]+$/i)
      idx = col_to_index(ref.upcase)
      source_cols = @metadata[:source]&.dig(:columns)
      return idx if source_cols && idx < source_cols.size
    end
    
    nil
  end

  def columnX(ref)
    idx = column_index(ref)
    idx ? "column#{idx}" : ref.to_s
  end

  def to_duckdb_sql
    sql = "SELECT * FROM data"
    
    @transformations.each do |t|
      case t[:type]
      when :select
        cols = t[:args].map { |c| columnX(c) }.join(', ')
        sql = sql.gsub('SELECT *', "SELECT #{cols}")
      when :filter, :where
        condition = translate_condition(t[:args].first)
        if sql.include?('WHERE')
          sql += " AND (#{condition})"
        else
          sql += " WHERE #{condition}"
        end
      when :limit
        sql += " LIMIT #{t[:args].first}"
      end
    end
    
    sql
  end

  def translate_condition(cond)
    result = cond.dup
    result = result.gsub('&&', 'AND').gsub('||', 'OR')
    result = result.gsub('==', '=').gsub('!=', '<>')
    
    @metadata[:source]&.dig(:columns)&.each_with_index do |col, idx|
      result = result.gsub(/\b#{Regexp.escape(col)}\b/, "column#{idx}")
      result = result.gsub(/\b#{index_to_col(idx)}\b/, "column#{idx}")
    end
    
    result
  end

  def inspect
    {
      store: @store,
      metadata: @metadata,
      transformations: @transformations,
      column_mapping: @column_mapping.keys
    }.inspect
  end
end

# ============================================================================
# Columnar IR - Pure Streaming with Enumerable
# ============================================================================

class ColumnarStream
  include Enumerable
  
  attr_reader :columns, :types, :config
  
  def initialize(columns, types, enum, config = nil)
    @columns = columns ? columns.compact.freeze : nil
    @types = types ? types.freeze : nil
    @enum = enum
    @config = config || CCVConfig.new
    
    if @columns && @config
      @config.set_metadata(:source, {}) unless @config.get_metadata(:source)
      @config.get_metadata(:source)[:columns] = @columns
      @config.get_metadata(:source)[:column_count] = @columns.size
      @config.build_column_mapping!
    end
  end

  def each(&block)
    @enum.each(&block)
  end

  def select(*keep_columns)
    keep_columns = keep_columns.map(&:to_s)
    
    if @columns && @columns.any?
      column_map = {}
      @columns.each_with_index do |col_name, idx|
        next if col_name.nil? || col_name.to_s.empty?
        col_name_str = col_name.to_s
        column_map[col_name_str] = idx
        column_map[col_name_str.downcase] = idx
        column_map[index_to_col(idx)] = idx
        column_map[index_to_col(idx).downcase] = idx
        column_map["column#{idx}"] = idx
      end
      
      indices = keep_columns.map do |col|
        column_map[col] || column_map[col.downcase]
      end.compact
      
      new_columns = indices.map { |i| @columns[i] }.compact
      new_types = @types ? indices.map { |i| @types[i] } : nil
    else
      indices = keep_columns.map { |c| col_to_index(c) }
      new_columns = keep_columns
      new_types = nil
    end
    
    new_enum = Enumerator.new do |yielder|
      @enum.each do |row|
        row_array = row.is_a?(Array) ? row : [row]
        selected = indices.map do |i|
          i && i < row_array.size ? row_array[i] : nil
        end
        if selected.size < keep_columns.size
          selected += [nil] * (keep_columns.size - selected.size)
        end
        yielder << selected
      end
    end
    
    ColumnarStream.new(new_columns, new_types, new_enum, @config)
  end

  def filter(&predicate)
    new_enum = Enumerator.new do |yielder|
      @enum.each do |row|
        begin
          context = RowContext.new(@columns, @types, row, @config)
          yielder << row if predicate.call(context)
        rescue => e
          warn "Filter error: #{e.message}" if $DEBUG
        end
      end
    end
    ColumnarStream.new(@columns, @types, new_enum, @config)
  end

  def limit(n)
    count = 0
    new_enum = Enumerator.new do |yielder|
      @enum.each do |row|
        if count < n
          yielder << row
          count += 1
        else
          break
        end
      end
    end
    ColumnarStream.new(@columns, @types, new_enum, @config)
  end

  def map(&block)
    new_enum = Enumerator.new do |yielder|
      @enum.each do |row|
        begin
          context = RowContext.new(@columns, @types, row, @config)
          yielder << block.call(context)
        rescue => e
          warn "Map error: #{e.message}" if $DEBUG
        end
      end
    end
    ColumnarStream.new(nil, nil, new_enum, @config)
  end

  def to_a
    @enum.to_a
  end
  
  def count
    @enum.count
  end
  
  def first(n = nil)
    n ? @enum.first(n) : @enum.first
  end
end

# ============================================================================
# RowContext - Provides access to row data via []
# ============================================================================

class RowContext
  attr_reader :columns, :row
  
  def initialize(columns, types, row, config)
    @columns = columns || []
    @types = types
    @row = row
    @config = config
  end

  def [](key)
    return nil if @row.nil?
    
    idx = if key.is_a?(Integer)
      key
    elsif @config
      @config.column_index(key)
    else
      case key
      when /^column(\d+)$/i
        $1.to_i
      when /^[A-Z]+$/i
        col_to_index(key.upcase)
      else
        @columns.index(key) || @columns.index { |c| c.to_s.downcase == key.to_s.downcase }
      end
    end

    return nil unless idx && idx < @row.size
    val = @row[idx]
    
    if @types && idx < @types.size
      case @types[idx]
      when :integer
        val.is_a?(String) ? val.to_i : val
      when :float
        val.is_a?(String) ? val.to_f : val
      else
        val
      end
    else
      val
    end
  end

  def method_missing(name, *args)
    self[name.to_s]
  end

  def respond_to_missing?(name, include_private = false)
    !self[name.to_s].nil?
  end
end

# ============================================================================
# XML/XLSX Templates Module
# ============================================================================

module XMLTemplates
  def self.escape(str)
    return '' if str.nil?
    str.to_s.gsub('&', '&amp;')
           .gsub('<', '&lt;')
           .gsub('>', '&gt;')
           .gsub('"', '&quot;')
           .gsub("'", '&apos;')
  end

  module XLSX
    def self.escape(str)
      XMLTemplates.escape(str)
    end

    CONTENT_TYPES = <<~XML.freeze
      <?xml version="1.0" encoding="UTF-8"?>
      <Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
        <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
        <Default Extension="xml" ContentType="application/xml"/>
        <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
        <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
      </Types>
    XML

    ROOT_RELS = <<~XML.freeze
      <?xml version="1.0" encoding="UTF-8"?>
      <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
        <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
      </Relationships>
    XML

    WORKBOOK_RELS = <<~XML.freeze
      <?xml version="1.0" encoding="UTF-8"?>
      <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
        <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
      </Relationships>
    XML

    def self.workbook(sheet_name = 'Sheet1')
      <<~XML
        <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
                  xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
          <sheets>
            <sheet name="#{escape(sheet_name)}" sheetId="1" r:id="rId1"/>
          </sheets>
        </workbook>
      XML
    end

    WORKSHEET_HEADER = <<~XML.freeze
      <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
      <worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
                 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
        <sheetData>
    XML

    def self.worksheet_footer(start_col, start_row, end_col, end_row)
      start_col_str = Helpers.index_to_col(start_col)
      end_col_str = Helpers.index_to_col(end_col)
      <<~XML
          </sheetData>
          <dimension ref="#{start_col_str}#{start_row}:#{end_col_str}#{end_row}"/>
        </worksheet>
      XML
    end

    def self.row(row_num, start_col, values)
      xml = %(<row r="#{row_num}">)
      values.each_with_index do |val, i|
        next if val.nil? || (val.is_a?(String) && val.empty?)
        col = Helpers.index_to_col(start_col + i)
        if val.is_a?(Numeric)
          xml << %(<c r="#{col}#{row_num}" t="n"><v>#{val}</v></c>)
        else
          escaped = escape(val)
          xml << %(<c r="#{col}#{row_num}" t="inlineStr"><is><t>#{escaped}</t></is></c>)
        end
      end
      xml << "</row>"
    end
  end

  module XML
    def self.escape(str)
      XMLTemplates.escape(str)
    end

    def self.document(root_tag = 'root')
      <<~XML
        <?xml version="1.0" encoding="UTF-8"?>
        <#{root_tag}>
        </#{root_tag}>
      XML
    end

    def self.row_from_array(row, columns, row_tag = 'row')
      xml = "<#{row_tag}>"
      columns.each_with_index do |col, i|
        val = row[i]
        xml << "<#{col}>#{escape(val)}</#{col}>" unless val.nil? || val.to_s.empty?
      end
      xml << "</#{row_tag}>"
    end

    def self.write_header(io, root_tag = 'root')
      io.puts '<?xml version="1.0" encoding="UTF-8"?>'
      io.puts "<#{root_tag}>"
    end

    def self.write_footer(io, root_tag = 'root')
      io.puts "</#{root_tag}>"
    end

    def self.write_row(io, row, columns, row_tag = 'row')
      io.puts row_from_array(row, columns, row_tag)
    end
  end

  module XLSXDimension
    def self.extract(sheet_xml_content)
      doc = Nokogiri::XML(sheet_xml_content)
      dimension = doc.at_xpath('//*[local-name()="dimension"]')
      
      return nil unless dimension && dimension['ref']
      
      ref = dimension['ref']
      if ref =~ /^([A-Z]+)(\d+):([A-Z]+)(\d+)$/
        {
          start_col: $1,
          start_row: $2.to_i,
          end_col: $3,
          end_row: $4.to_i,
          start_col_idx: Helpers.col_to_index($1),
          end_col_idx: Helpers.col_to_index($3),
          column_count: Helpers.col_to_index($3) - Helpers.col_to_index($1) + 1,
          row_count: $4.to_i - $2.to_i + 1
        }
      else
        nil
      end
    end

    def self.sheet_names(zip)
      sheet_names = {}
      workbook_entry = zip.find { |e| e.name =~ /xl\/workbook\.xml$/ }
      
      if workbook_entry
        content = workbook_entry.get_input_stream.read
        doc = Nokogiri::XML(content)
        doc.remove_namespaces!
        doc.xpath('//sheets/sheet').each do |sheet|
          sheet_id = sheet['sheetId'].to_i
          sheet_name = sheet['name']
          sheet_names[sheet_id] = sheet_name if sheet_id && sheet_name
        end
      end
      
      sheet_names
    end

    def self.find_sheet_id(sheet_names, target)
      return nil if target.nil?
      
      sheet_names.each do |id, name|
        return id if name == target
      end
      
      if target =~ /^Sheet(\d+)$/i
        id = $1.to_i
        return id if sheet_names.key?(id)
      end
      
      nil
    end
  end
end

# ============================================================================
# Streaming XLSX Reader
# ============================================================================

class StreamingXLSXReader
  include Enumerable
  include Helpers
  include XMLTemplates::XLSXDimension

  attr_reader :columns, :actual_columns, :actual_rows, :dimension, :config

  def initialize(path, config, sheet: nil, range: nil)
    @path = path
    @config = config
    @sheet = sheet
    @range = range
    @shared_strings = []
    @dimension = nil
    @actual_columns = 16384
    @actual_rows = 1_048_576
    @columns = nil
    @sheet_id = nil
    @headers_processed = false
    
    scan_metadata!
    
    @config.set_metadata(:source, {
      format: :xlsx,
      path: path,
      sheet: @sheet,
      range: @range,
      actual_columns: @actual_columns,
      actual_rows: @actual_rows,
      dimension: @dimension,
      sheet_id: @sheet_id
    })
    @config.set(:source_format, :xlsx)
  end

  def each(&block)
    return enum_for(:each) unless block_given?
    
    range_info = @range ? parse_range(@range) : nil
    start_row = range_info ? range_info[:start_row] : 1
    end_row = range_info ? range_info[:end_row] : @actual_rows
    start_col_idx = range_info ? range_info[:start_col] : 0
    end_col_idx = range_info ? range_info[:end_col] : @actual_columns - 1
    
    col_count = end_col_idx - start_col_idx + 1
    
    headers = @config.get(:headers, true)
    has_transformations = @config.transformations.any? || @config.get(:query)
    trim = @config.get(:trim, true)
    
    puts "DEBUG: Starting XLSX read with headers=#{headers}, has_transformations=#{has_transformations}, range=#{start_row}-#{end_row}, cols=#{start_col_idx}-#{end_col_idx}" if $DEBUG
    
    parser = Nokogiri::XML::SAX::Parser.new(XLSXHandler.new(@shared_strings) do |row_data, row_num|
      puts "DEBUG: Got row #{row_num} from parser: #{row_data.inspect}" if $DEBUG
      next if row_num < start_row || row_num > end_row
      
      row_array = []
      (start_col_idx..end_col_idx).each do |i|
        col_letter = index_to_col(i)
        val = row_data[col_letter].to_s
        row_array << val
      end
      
      if trim
        while row_array.last && row_array.last.empty?
          row_array.pop
        end
      end
      
      puts "DEBUG: Row array: #{row_array.inspect}" if $DEBUG
      
      if !has_transformations
        puts "DEBUG: Direct I/O - yielding row as-is" if $DEBUG
        yield row_array
        next
      end
      
      if headers
        if !@headers_processed
          @columns = row_array
          puts "DEBUG: Using headers from first row: #{@columns.inspect}" if $DEBUG
          @headers_processed = true
          next
        else
          puts "DEBUG: Yielding data row for transformations: #{row_array.inspect}" if $DEBUG
          yield row_array
        end
      else
        if @columns.nil?
          @columns = (start_col_idx..end_col_idx).map { |i| index_to_col(i) }
          if trim
            while @columns.last && row_array.last && row_array.last.empty?
              @columns.pop
            end
          end
          puts "DEBUG: No headers mode, using generated column letters: #{@columns.inspect}" if $DEBUG
        end
        puts "DEBUG: Yielding data row for transformations: #{row_array.inspect}" if $DEBUG
        yield row_array
      end
    end)
    
    begin
      Zip::File.open(@path) do |zip|
        sheet_entry = zip.find { |e| e.name =~ /xl\/worksheets\/sheet#{@sheet_id}\.xml$/ }
        if sheet_entry
          puts "DEBUG: Streaming XLSX sheet #{@sheet_id}" if $DEBUG
          parser.parse(sheet_entry.get_input_stream)
        else
          warn "Warning: Sheet #{@sheet_id} not found"
        end
      end
    rescue Errno::EINVAL
      puts "DEBUG: Retrying XLSX read" if $DEBUG
      Zip::File.open(@path) do |zip|
        sheet_entry = zip.find { |e| e.name =~ /xl\/worksheets\/sheet#{@sheet_id}\.xml$/ }
        if sheet_entry
          parser.parse(sheet_entry.get_input_stream)
        end
      end
    end
    
    if @columns
      @config.set_metadata(:source, @config.get_metadata(:source).merge({
        columns: @columns,
        column_count: @columns.size
      }))
      @config.set(:source_columns, @columns)
      @config.set(:source_column_count, @columns.size)
      @config.build_column_mapping!
    end
    
    puts "DEBUG: Finished reading XLSX" if $DEBUG
  end

  private

  def scan_metadata!
    begin
      Zip::File.open(@path) do |zip|
        sheet_names = XMLTemplates::XLSXDimension.sheet_names(zip)
        @sheet_id = XMLTemplates::XLSXDimension.find_sheet_id(sheet_names, @sheet || 'Sheet1') || 1
        
        sheet_entry = zip.find { |e| e.name =~ /xl\/worksheets\/sheet#{@sheet_id}\.xml$/ }
        if sheet_entry
          content = sheet_entry.get_input_stream.read
          dim = XMLTemplates::XLSXDimension.extract(content)
          if dim
            @dimension = dim
            @actual_columns = dim[:column_count]
            @actual_rows = dim[:row_count]
            puts "DEBUG: XLSX dimension: #{@actual_columns} columns, #{@actual_rows} rows" if $DEBUG
          end
        end
        
        shared_entry = zip.find { |e| e.name =~ /xl\/sharedStrings\.xml$/ }
        if shared_entry
          puts "DEBUG: Loading shared strings" if $DEBUG
          content = shared_entry.get_input_stream.read
          doc = Nokogiri::XML(content)
          doc.remove_namespaces!
          @shared_strings = doc.xpath('//si').map do |si|
            t_elements = si.xpath('.//t')
            t_elements.map(&:text).join
          end
          puts "DEBUG: Loaded #{@shared_strings.size} shared strings" if $DEBUG
        end
      end
    rescue Errno::EINVAL
      puts "DEBUG: Retrying scan_metadata!" if $DEBUG
      Zip::File.open(@path) do |zip|
        sheet_names = XMLTemplates::XLSXDimension.sheet_names(zip)
        @sheet_id = XMLTemplates::XLSXDimension.find_sheet_id(sheet_names, @sheet || 'Sheet1') || 1
        
        sheet_entry = zip.find { |e| e.name =~ /xl\/worksheets\/sheet#{@sheet_id}\.xml$/ }
        if sheet_entry
          content = sheet_entry.get_input_stream.read
          dim = XMLTemplates::XLSXDimension.extract(content)
          if dim
            @dimension = dim
            @actual_columns = dim[:column_count]
            @actual_rows = dim[:row_count]
          end
        end
        
        shared_entry = zip.find { |e| e.name =~ /xl\/sharedStrings\.xml$/ }
        if shared_entry
          content = shared_entry.get_input_stream.read
          doc = Nokogiri::XML(content)
          doc.remove_namespaces!
          @shared_strings = doc.xpath('//si').map do |si|
            t_elements = si.xpath('.//t')
            t_elements.map(&:text).join
          end
        end
      end
    end
    
    @columns = (0...@actual_columns).map { |i| index_to_col(i) }
  end

  class XLSXHandler < Nokogiri::XML::SAX::Document
    def initialize(shared_strings, &block)
      @shared_strings = shared_strings
      @block = block
      @current_row = {}
      @row_num = 0
      @col_num = 0
      @in_value = false
      @value = +''
      @current_cell = nil
    end
  
    def start_element(name, attrs = [])
      case name
      when 'row'
        @current_row = {}
        attrs_hash = Hash[attrs]
        @row_num = attrs_hash['r'].to_i if attrs_hash['r']
        @col_num = 0
      when 'c'
        attrs_hash = Hash[attrs]
        @current_cell = { 
          type: attrs_hash['t'],
          r: attrs_hash['r']
        }
        if attrs_hash['r'] && attrs_hash['r'] =~ /^([A-Z]+)/
          @col_num = Helpers.col_to_index($1)
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
      case name
      when 'v', 't', 'is'
        @in_value = false
        if @current_cell && @current_cell[:type] == 's' && @shared_strings
          idx = @value.to_i
          @current_row[@col_num] = @shared_strings[idx] if idx && @shared_strings[idx]
        else
          @current_row[@col_num] = @value
        end
      when 'row'
        row_hash = {}
        @current_row.each do |col_idx, val|
          row_hash[Helpers.index_to_col(col_idx)] = val
        end
        @block.call(row_hash, @row_num) unless row_hash.empty?
        @current_row = {}
      end
    end
  end
end

# ============================================================================
# Streaming XLSX Writer
# ============================================================================

class StreamingXLSXWriter
  include Helpers

  attr_reader :path, :config

  def initialize(path, config, sheet: 'Sheet1')
    @path = path
    @config = config
    @sheet = sheet
    @temp_file = nil
  end

  def workbook(sheet_name = 'Sheet1')
    XMLTemplates::XLSX.workbook(sheet_name)
  end

  def write(stream, start_cell: 'A1', headers: true, trim: false)
    start_col, start_row = parse_start_cell(start_cell)
    
    columns = @config.get(:source_columns) || stream.columns
    actual_cols = if trim && @config.get_metadata(:source)&.dig(:actual_columns)
      @config.get_metadata(:source)[:actual_columns]
    else
      columns&.size || 1
    end

    @temp_file = File.join(File.dirname(@path), ".tmp_#{Time.now.to_i}_#{rand(100000)}.xlsx")
    
    puts "DEBUG: Creating XLSX at #{@path} with #{actual_cols} columns" if $DEBUG

    begin
      Zip::OutputStream.open(@temp_file) do |zos|
        zos.put_next_entry('[Content_Types].xml')
        zos.write(XMLTemplates::XLSX::CONTENT_TYPES)

        zos.put_next_entry('_rels/.rels')
        zos.write(XMLTemplates::XLSX::ROOT_RELS)

        zos.put_next_entry('xl/_rels/workbook.xml.rels')
        zos.write(XMLTemplates::XLSX::WORKBOOK_RELS)

        zos.put_next_entry('xl/workbook.xml')
        zos.write(workbook(@sheet))

        zos.put_next_entry('xl/worksheets/sheet1.xml')
        zos.write(XMLTemplates::XLSX::WORKSHEET_HEADER)

        current_row = start_row
        buffer = StringIO.new

        if headers && columns&.any?
          buffer.write(XMLTemplates::XLSX.row(current_row, start_col, columns))
          current_row += 1
        end

        row_count = 0
        stream.each do |row|
          trimmed_row = trim ? row[0...actual_cols] : row
          formatted_row = trimmed_row.map { |v| v.is_a?(Numeric) ? v : v.to_s }
          buffer.write(XMLTemplates::XLSX.row(current_row, start_col, formatted_row))
          current_row += 1
          row_count += 1

          if buffer.size > 65536
            zos.write(buffer.string)
            buffer = StringIO.new
          end
        end

        zos.write(buffer.string) unless buffer.size.zero?

        last_row = current_row - 1
        last_col = start_col + actual_cols - 1
        zos.write(XMLTemplates::XLSX.worksheet_footer(start_col, start_row, last_col, last_row))
        
        puts "DEBUG: XLSX wrote #{row_count} rows" if $DEBUG
      end

      File.rename(@temp_file, @path)
      puts "DEBUG: Successfully created #{@path}" if $DEBUG

    rescue => e
      FileUtils.rm_f(@temp_file) if @temp_file && File.exist?(@temp_file)
      raise e
    end
  end

  private

  def parse_start_cell(cell)
    if cell =~ /^([A-Z]+)(\d+)$/
      [col_to_index($1), $2.to_i]
    else
      [0, 1]
    end
  end
end

# ============================================================================
# Streaming XML Reader
# ============================================================================

class StreamingXMLReader
  include Enumerable
  include Helpers

  attr_reader :columns, :config

  def initialize(path, config, record_path: '//row')
    @path = path
    @config = config
    @record_path = record_path
    @columns = nil
    
    scan_columns!
    
    @config.set_metadata(:source, {
      format: :xml,
      path: path,
      columns: @columns,
      record_path: record_path
    })
    @config.set(:source_format, :xml)
    @config.set(:source_columns, @columns)
    @config.set(:source_column_count, @columns.size)
    @config.build_column_mapping!
  end

  def each(&block)
    return enum_for(:each) unless block_given?
    
    columns = @columns
    trim = @config.get(:trim, true)
    
    parser = Nokogiri::XML::SAX::Parser.new(XMLHandler.new do |row_data|
      row_array = columns.map { |c| row_data[c].to_s }
      
      if trim
        while row_array.last && row_array.last.empty?
          row_array.pop
        end
      end
      
      yield row_array
    end)
    
    if @path == '-'
      parser.parse($stdin)
    else
      File.open(@path) { |f| parser.parse(f) }
    end
  end

  private

  def scan_columns!
    catch :got_columns do
      parser = Nokogiri::XML::SAX::Parser.new(FirstRowHandler.new do |row_data|
        @columns = row_data.keys
        throw :got_columns
      end)
      
      if @path == '-'
        @columns = ['col0', 'col1', 'col2']
      else
        File.open(@path) { |f| parser.parse(f) }
      end
    end
    
    @columns ||= ['col0', 'col1', 'col2']
  end

  class XMLHandler < Nokogiri::XML::SAX::Document
    def initialize(&block)
      @block = block
      @current_record = {}
      @current_element = nil
      @depth = 0
      @in_record = false
      @target_depth = 2
      @current_text = +''
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
      @current_text << text if @in_record && @current_element
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

  class FirstRowHandler < Nokogiri::XML::SAX::Document
    def initialize(&block)
      @block = block
      @current_record = {}
      @depth = 0
      @got_row = false
      @current_element = nil
      @current_text = +''
    end

    def start_element(name, attrs = [])
      @depth += 1
      return if @got_row
      @current_element = name if @depth == 2
      @current_text = +''
    end

    def characters(text)
      return if @got_row
      @current_text << text if @depth == 2 && @current_element
    end

    def end_element(name)
      return if @got_row
      if @depth == 2 && @current_element == name
        @current_record[@current_element] = @current_text.strip
      end
      
      if @depth == 2 && name == 'row'
        @block.call(@current_record)
        @got_row = true
      end
      @depth -= 1
    end
  end
end

# ============================================================================
# Streaming XML Writer
# ============================================================================

class StreamingXMLWriter
  include Helpers
  include XMLTemplates::XML

  attr_reader :path, :config

  def initialize(path, config, root_tag: 'root', row_tag: 'row')
    @path = path
    @config = config
    @root_tag = root_tag
    @row_tag = row_tag
  end

  def write(stream, pretty: false)
    columns = @config.get(:source_columns) || stream.columns
    trim = @config.get(:trim, true)
    
    File.open(@path, 'w') do |f|
      write_header(f, @root_tag)

      row_count = 0
      stream.each do |row|
        if trim
          while row.last && row.last.empty?
            row.pop
          end
        end
        
        if columns
          write_row(f, row, columns, @row_tag)
        else
          generic_columns = (0...row.size).map { |i| "col#{i}" }
          write_row(f, row, generic_columns, @row_tag)
        end
        row_count += 1
      end

      write_footer(f, @root_tag)
      
      puts "DEBUG: XML wrote #{row_count} rows to #{@path}" if $DEBUG
    end
  end
end

# ============================================================================
# Readers
# ============================================================================

module Reader
  def self.detect_type(value)
    return :string if value.nil? || value.to_s.empty?
    s = value.to_s.strip
    return :integer if s.match?(/^-?\d+$/)
    return :float if s.match?(/^-?\d*\.\d+$/)
    :string
  end

  def self.cast(value, type)
    return nil if value.nil?
    return value if value.is_a?(Numeric) && type != :string

    case type
    when :integer
      value.to_s =~ /^-?\d+$/ ? value.to_i : value
    when :float
      value.to_s =~ /^-?\d*\.?\d+$/ ? value.to_f : value
    else
      value.to_s
    end
  end

  def self.csv(path, config, headers: nil)
    return stdin_csv(config, headers: headers) if path == '-'

    columns = nil
    types = nil
    
    headers = true if headers.nil?
    
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
      header_skipped = false
      
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
      end
    end

    config.set_metadata(:source, {
      format: :csv,
      path: path,
      columns: columns,
      types: types
    })
    config.set(:source_format, :csv)
    config.set(:source_columns, columns)
    config.set(:source_column_count, columns&.size || 0)
    config.build_column_mapping!

    ColumnarStream.new(columns, types, row_enum, config)
  end

  def self.stdin_csv(config, headers: nil)
    lines = $stdin.each_line.map(&:chomp).reject(&:empty?)
    return ColumnarStream.new(nil, nil, [].each, config) if lines.empty?

    parsed_lines = lines.map { |line| CSV.parse_line(line) }.compact
    return ColumnarStream.new(nil, nil, [].each, config) if parsed_lines.empty?

    first_row = parsed_lines.first
    
    if headers.nil?
      if first_row.all? { |v| v.is_a?(String) && v.to_s.match?(/^[A-Za-z]/) }
        headers = true
        puts "DEBUG: Auto-detected headers=true" if $DEBUG
      else
        headers = false
        puts "DEBUG: Auto-detected headers=false" if $DEBUG
      end
    end

    if headers
      columns = first_row.map(&:to_s)
      data_rows = parsed_lines[1..-1] || []
      
      if data_rows.any?
        types = data_rows.first.map { |v| detect_type(v) }
      else
        types = nil
      end
      
      row_enum = Enumerator.new do |yielder|
        data_rows.each do |row|
          if types
            typed_row = row.each_with_index.map { |val, i| cast(val, types[i]) }
            yielder << typed_row
          else
            yielder << row
          end
        end
      end
      
      config.set_metadata(:source, { format: :csv, columns: columns, types: types })
      config.set(:source_columns, columns)
      config.build_column_mapping!
      
      ColumnarStream.new(columns, types, row_enum, config)
    else
      columns = (0...first_row.size).map { |i| index_to_col(i) }
      types = first_row.map { |v| detect_type(v) }
      
      row_enum = Enumerator.new do |yielder|
        parsed_lines.each do |row|
          typed_row = row.each_with_index.map { |val, i| cast(val, types[i]) }
          yielder << typed_row
        end
      end
      
      config.set_metadata(:source, { format: :csv, columns: columns, types: types })
      config.set(:source_columns, columns)
      config.build_column_mapping!
      
      ColumnarStream.new(columns, types, row_enum, config)
    end
  end

  def self.xlsx(path, config, sheet: nil, range: nil, headers: true)
    reader = StreamingXLSXReader.new(path, config, sheet: sheet, range: range)
    ColumnarStream.new(reader.columns, nil, reader.each, config)
  end

  def self.xml(path, config, record_path: '//row', headers: true)
    reader = StreamingXMLReader.new(path, config, record_path: record_path)
    ColumnarStream.new(reader.columns, nil, reader.each, config)
  end

  def self.auto(path, config, **options)
    headers = if options.key?(:headers)
      options[:headers]
    elsif path == '-'
      nil
    else
      true
    end
    
    sheet = options[:sheet]
    range = options[:range]
    record_path = options.fetch(:record_path, '//row')
    
    puts "DEBUG: auto called with path=#{path}, headers=#{headers.inspect}, sheet=#{sheet}, range=#{range}" if $DEBUG
    
    case path
    when /\.csv$/i, '-'
      csv(path, config, headers: headers)
    when /\.xlsx?$/i
      xlsx(path, config, headers: headers, sheet: sheet, range: range)
    when /\.xml$/i
      xml(path, config, headers: headers, record_path: record_path)
    else
      csv(path, config, headers: headers)
    end
  end
end

# ============================================================================
# Writers
# ============================================================================

module Writer
  include Helpers

  def self.csv(stream, path = nil, output_headers: true, trim_empty_columns: false)
    puts "DEBUG: Writer.csv - output_headers=#{output_headers}, trim_empty_columns=#{trim_empty_columns}" if $DEBUG

    columns = stream.columns
    config = stream.respond_to?(:config) ? stream.config : nil
    
    actual_cols = if trim_empty_columns && config && config.get_metadata(:source)&.dig(:actual_columns)
      config.get_metadata(:source)[:actual_columns]
    else
      columns&.size || 0
    end

    io = path && path != '-' ? File.open(path, 'w') : $stdout

    begin
      if output_headers && columns&.any?
        header_row = trim_empty_columns ? columns[0...actual_cols] : columns
        io.puts(header_row.join(','))
      end

      stream.each do |row|
        if trim_empty_columns
          while row.last && row.last.empty?
            row.pop
          end
        end
        trimmed_row = trim_empty_columns ? row[0...actual_cols] : row
        formatted_row = trimmed_row.map { |v| v.is_a?(Numeric) ? format_number(v) : v.to_s }
        io.puts(formatted_row.join(','))
      end
    ensure
      io.close if io != $stdout
    end
  end

  def self.xlsx(stream, path = nil, sheet: 'Sheet1', start_cell: 'A1', 
                output_headers: false, trim_empty_columns: false)
    raise "XLSX output requires a file path" if path.nil? || path == '-'
    
    config = stream.respond_to?(:config) ? stream.config : CCVConfig.new
    
    writer = StreamingXLSXWriter.new(path, config, sheet: sheet)
    writer.write(stream, start_cell: start_cell, headers: output_headers, trim: trim_empty_columns)
  end

  def self.xml(stream, path = nil, root_tag: 'root', row_tag: 'row', output_headers: true)
    config = stream.respond_to?(:config) ? stream.config : CCVConfig.new
    writer = StreamingXMLWriter.new(path, config, root_tag: root_tag, row_tag: row_tag)
    writer.write(stream)
  end

  def self.jsonl(stream, path = nil, output_headers: true, trim_empty_columns: false)
    columns = stream.columns
    config = stream.respond_to?(:config) ? stream.config : nil
    
    actual_cols = if trim_empty_columns && config && config.get_metadata(:source)&.dig(:actual_columns)
      config.get_metadata(:source)[:actual_columns]
    else
      columns&.size || 0
    end

    io = path && path != '-' ? File.open(path, 'w') : $stdout

    begin
      stream.each do |row|
        if trim_empty_columns
          while row.last && row.last.empty?
            row.pop
          end
        end
        trimmed_row = trim_empty_columns ? row[0...actual_cols] : row
        
        if output_headers && columns&.any?
          cols_to_use = trim_empty_columns ? columns[0...actual_cols] : columns
          output = {}
          cols_to_use.each_with_index do |col, i|
            val = i < trimmed_row.size ? trimmed_row[i] : nil
            output[col] = val.is_a?(Numeric) ? val.to_s : val.to_s
          end
          io.puts(output.to_json)
        else
          output = {}
          trimmed_row.each_with_index do |val, i|
            output[index_to_col(i)] = val.is_a?(Numeric) ? val.to_s : val.to_s
          end
          io.puts(output.to_json)
        end
      end
    ensure
      io.close if io != $stdout
    end
  end

  def self.auto(stream, path = nil, **options)
    output_headers = options.fetch(:output_headers, true)
    mode = options.fetch(:mode, 'append')
    trim_empty_columns = options.fetch(:trim_empty_columns, true)
    
    puts "DEBUG: Writer.auto: path=#{path}, output_headers=#{output_headers}, mode=#{mode}, trim=#{trim_empty_columns}" if $DEBUG
    
    case path
    when /\.xlsx?$/i
      sheet = options[:sheet] || 'Sheet1'
      start_cell = options[:start_cell] || 'A1'
      xlsx(stream, path, sheet: sheet, start_cell: start_cell, 
           output_headers: output_headers, trim_empty_columns: trim_empty_columns)
    when /\.xml$/i
      root_tag = options[:root_tag] || 'root'
      row_tag = options[:row_tag] || 'row'
      xml(stream, path, root_tag: root_tag, row_tag: row_tag, output_headers: output_headers)
    when /\.jsonl?$/i
      jsonl(stream, path, output_headers: output_headers, trim_empty_columns: trim_empty_columns)
    else
      csv(stream, path, output_headers: output_headers, trim_empty_columns: trim_empty_columns)
    end
  end

  def self.clear_xlsx_range(path, sheet: 'Sheet1', range:)
    warn "Warning: clear_xlsx_range not fully implemented"
  end
end

# ============================================================================
# DuckDB Engine
# ============================================================================

module DuckDBEngine
  module_function
  include Helpers

  BATCH_SIZE = 40

  def self.query(stream, sql, config)
    return enum_for(:query, stream, sql, config) unless block_given?

    pipe_path = "/tmp/ccv_pipe_#{Process.pid}_#{Time.now.to_i}_#{rand(1000)}"
    writer_ready = Queue.new
    writer_exception = nil

    begin
      puts "DEBUG: Creating pipe at #{pipe_path}" if $DEBUG
      system("mkfifo #{pipe_path}")

      writer_thread = Thread.new do
        begin
          puts "DEBUG: Writer thread started" if $DEBUG
          
          File.open(pipe_path, 'w') do |pipe|
            puts "DEBUG: Pipe opened for writing" if $DEBUG
            writer_ready.push(:opened)
            
            columns = config.get(:source_columns)
            col_count = config.get(:source_column_count, 1)
            
            if columns&.any?
              headers = col_count.times.map { |i| "column#{i}" }.join(',')
              puts "DEBUG: Writing headers: #{headers}" if $DEBUG
              pipe.puts(headers)
            end

            batch = []
            row_count = 0
            stream.each do |row|
              formatted = row.map { |v| v.is_a?(Numeric) ? format_number(v) : v.to_s }
              batch << formatted.join(',')
              row_count += 1
              
              if batch.size >= BATCH_SIZE
                pipe.write(batch.join("\n") + "\n")
                batch.clear
              end
            end
            pipe.write(batch.join("\n") + "\n") unless batch.empty?
            puts "DEBUG: Writer thread finished, total rows: #{row_count}" if $DEBUG
          end
        rescue Errno::EPIPE
          puts "DEBUG: Pipe closed by reader (normal for LIMIT)" if $DEBUG
          writer_ready.push(:closed) unless writer_ready.closed?
        rescue => e
          puts "DEBUG: Writer thread exception: #{e.class} - #{e.message}" if $DEBUG
          writer_exception = e
          writer_ready.push(:error) unless writer_ready.closed?
        end
      end

      signal = nil
      begin
        Timeout.timeout(5) do
          signal = writer_ready.pop
        end
      rescue Timeout::Error
        puts "DEBUG: Timeout waiting for pipe to open" if $DEBUG
        raise "Timeout waiting for pipe to open"
      end

      if signal == :error
        raise writer_exception if writer_exception
        raise "Failed to open pipe"
      end

      puts "DEBUG: Pipe opened, starting DuckDB" if $DEBUG

      columns = config.get(:source_columns)
      col_count = config.get(:source_column_count, 1)
      has_headers = !!(columns&.any?)

      columns_def = col_count.times.map { |i| "'column#{i}': 'VARCHAR'" }.join(', ')
      csv_options = [
        "HEADER=#{has_headers}",
        "COLUMNS={#{columns_def}}",
        "PARALLEL=false",
        "BUFFER_SIZE=65536"
      ].join(', ')

      if sql.strip =~ /^SELECT/i
        final_sql = sql.gsub(/\bfrom\s+data\b/i, "FROM read_csv('#{pipe_path}', #{csv_options})")
      elsif sql.strip.empty?
        final_sql = "SELECT * FROM read_csv('#{pipe_path}', #{csv_options})"
      else
        final_sql = "SELECT * FROM read_csv('#{pipe_path}', #{csv_options}) WHERE #{sql}"
      end

      puts "DEBUG: DuckDB SQL: #{final_sql}" if $DEBUG

      DuckDB::Database.open(nil) do |db|
        db.connect do |conn|
          result = conn.execute(final_sql)

          result_columns = result.columns.map(&:name)
          if columns
            result_columns = result_columns.map do |col|
              if col =~ /^column(\d+)$/
                idx = $1.to_i
                columns[idx] || col
              else
                col
              end
            end
          end

          config.set_metadata(:result, {
            columns: result_columns,
            row_count: 0
          })

          row_count = 0
          result.each do |row|
            yield row.to_a.map { |v| v.is_a?(Numeric) ? v.to_s : v.to_s }
            row_count += 1
          end

          config.get_metadata(:result)[:row_count] = row_count
          puts "DEBUG: DuckDB finished, total result rows: #{row_count}" if $DEBUG
        end
      end

      writer_thread.join
    ensure
      File.unlink(pipe_path) if File.exist?(pipe_path)
      puts "DEBUG: Pipe removed" if $DEBUG
    end
  end
end

# ============================================================================
# Configuration and Pipeline
# ============================================================================

Config = Struct.new(
  :input, :output, :query, :operations, :format,
  :sheet, :range, :start_cell, :debug, :clear_range,
  :headers, :mode, :record_path, :output_headers, :trim, keyword_init: true
) do
  def self.from_args(args)
    new(operations: [], headers: true, mode: 'append', record_path: '//row', 
        output_headers: true, trim: true).tap { |c| Parser.parse(args, c) }
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
    puts "DEBUG: Parsing args: #{args.inspect}" if $DEBUG

    i = 0
    last_command = nil
    
    while i < args.length
      token = args[i]
      puts "DEBUG: Processing arg #{i}: #{token}" if $DEBUG

      case token
      when 'from'
        config.input = args[i + 1] if i + 1 < args.length
        puts "DEBUG: from -> input = #{config.input}" if $DEBUG
        i += 2
      when 'to'
        config.output = args[i + 1] if i + 1 < args.length
        puts "DEBUG: to -> output = #{config.output}" if $DEBUG
        i += 2
      when 'clear'
        config.clear_range = true
        puts "DEBUG: clear mode enabled" if $DEBUG
        i += 1
      when 'sheet'
        config.sheet = args[i + 1] if i + 1 < args.length
        puts "DEBUG: sheet = #{config.sheet}" if $DEBUG
        i += 2
        last_command = 'sheet'
      when 'range'
        config.range = args[i + 1] if i + 1 < args.length
        puts "DEBUG: range = #{config.range}" if $DEBUG
        i += 2
        last_command = 'range'
      when 'record_path'
        config.record_path = args[i + 1] if i + 1 < args.length
        puts "DEBUG: record_path = #{config.record_path}" if $DEBUG
        i += 2
      when 'start'
        config.start_cell = args[i + 1] if i + 1 < args.length
        puts "DEBUG: start_cell = #{config.start_cell}" if $DEBUG
        i += 2
      when 'mode'
        config.mode = args[i + 1] if i + 1 < args.length
        puts "DEBUG: mode = #{config.mode}" if $DEBUG
        i += 2
      when 'select'
        cols = []
        i += 1
        while i < args.length && !args[i].start_with?('-') && 
              !%w[from to sheet range filter where limit sql query record_path start mode clear].include?(args[i])
          cols << args[i]
          i += 1
        end
        config.operations << { type: :select, args: cols } unless cols.empty?
        puts "DEBUG: select columns: #{cols.inspect}" if $DEBUG
      when 'filter', 'where'
        config.operations << { type: :filter, args: [args[i + 1]] }
        puts "DEBUG: filter condition: #{args[i + 1]}" if $DEBUG
        i += 2
      when 'limit'
        config.operations << { type: :limit, args: [args[i + 1].to_i] }
        puts "DEBUG: limit: #{args[i + 1]}" if $DEBUG
        i += 2
      when '-q', '--query'
        config.query = args[i + 1] if i + 1 < args.length
        puts "DEBUG: query: #{config.query}" if $DEBUG
        i += 2
      when '--no-output-headers'
        config.output_headers = false
        puts "DEBUG: output headers disabled" if $DEBUG
        i += 1
      when '--output-headers'
        config.output_headers = true
        puts "DEBUG: output headers enabled" if $DEBUG
        i += 1
      when '--no-headers'
        config.headers = false
        puts "DEBUG: input headers disabled" if $DEBUG
        i += 1
      when '--headers'
        config.headers = true
        puts "DEBUG: input headers enabled" if $DEBUG
        i += 1
      when '--trim'
        config.trim = true
        puts "DEBUG: trim enabled" if $DEBUG
        i += 1
      when '--debug'
        config.debug = true
        $DEBUG = true
        puts "DEBUG: debug mode enabled"
        i += 1
      else
        if token.start_with?('-')
          i += 1
        elsif config.clear_range
          if config.output.nil?
            config.output = token
            puts "DEBUG: clear output = #{token}" if $DEBUG
          elsif config.input.nil?
            config.input = token
            puts "DEBUG: clear input = #{token}" if $DEBUG
          end
          i += 1
        else
          if config.input.nil? && !%w[sheet range].include?(last_command)
            config.input = token
            puts "DEBUG: implicit input = #{token}" if $DEBUG
          end
          i += 1
        end
      end
    end

    if config.clear_range && config.output.nil?
      warn "Error: No file specified for clear operation"
      exit 1
    end

    if config.input.nil? && !config.input_stream? && !config.clear_range && args.any? && !args.include?('--help')
      puts "DEBUG: No input found, showing help" if $DEBUG
      show_help
      exit 0
    end

    puts "DEBUG: Final config: #{config.inspect}" if $DEBUG
  end

  def show_help
    puts <<~HELP
      Usage: ccv [input] [to output] [commands] [options]

      7 SIMPLEST COMMANDS (80% of use):

        1. READ:        ccv data.xlsx
        2. CONVERT:     ccv data.csv to output.xlsx
        3. SELECT:      ccv data.csv select Name Age
        4. FILTER:      ccv data.csv filter "Age > 25"
        5. LIMIT:       ccv data.csv limit 10
        6. SQL:         ccv data.csv -q "SELECT Name FROM data"
        7. CLEAR:       ccv clear data.xlsx range A1:B2

      EXAMPLES:
        ccv data.xlsx sheet Sheet1 range A1:B10
        ccv data.csv to output.xlsx start A1 mode append
        ccv --no-headers data.xlsx filter "B::INT > 25"
        cat data.csv | ccv - to output.xlsx

      COMMANDS:
        select COLUMN...     Choose columns
        filter CONDITION     Filter rows
        limit N              Max rows
        -q "SQL"             SQL query

      MODIFIERS:
        sheet NAME           Excel sheet
        range CELLS          Excel range
        start CELL           Output start cell
        mode MODE            Write mode (append/overwrite)

      FLAGS:
        --no-headers         Input has no headers
        --headers            Input has headers (default)
        --no-output-headers  Don't write headers
        --output-headers     Write headers (default)
        --trim              Remove trailing empty columns (default)
        --debug             Show debug info
    HELP
  end
end

class Pipeline
  attr_reader :config

  def initialize
    @config = CCVConfig.new
  end

  def run(args)
    cli_config = Config.from_args(args)
    
    if args.include?('--help') || args.empty?
      Parser.show_help
      return
    end
    
    @config.set(:input, cli_config.input)
    @config.set(:output, cli_config.output)
    @config.set(:query, cli_config.query)
    @config.set(:sheet, cli_config.sheet)
    @config.set(:range, cli_config.range)
    @config.set(:start_cell, cli_config.start_cell)
    @config.set(:headers, cli_config.headers)
    @config.set(:mode, cli_config.mode)
    @config.set(:record_path, cli_config.record_path)
    @config.set(:output_headers, cli_config.output_headers)
    @config.set(:trim, cli_config.trim)
    
    cli_config.operations.each do |op|
      @config.add_transform(op[:type], *op[:args])
    end

    puts "DEBUG: Pipeline running with config: #{@config.inspect}" if $DEBUG

    if cli_config.clear_range
      puts "DEBUG: Running clear operation" if $DEBUG
      Writer.clear_xlsx_range(
        cli_config.output,
        sheet: cli_config.sheet || 'Sheet1',
        range: cli_config.range
      )
      return
    end

    puts "DEBUG: Reading input" if $DEBUG
    stream = read_input(cli_config)

    has_transformations = (cli_config.operations&.any?) || cli_config.query

    if has_transformations
      sql = if cli_config.query
        cli_config.query
      else
        @config.to_duckdb_sql
      end
      
      puts "DEBUG: Built SQL: #{sql}" if $DEBUG

      row_queue = Queue.new
      writer_exception = nil

      writer_thread = Thread.new do
        begin
          write_output_streaming(row_queue, cli_config)
        rescue => e
          writer_exception = e
        ensure
          row_queue.close
        end
      end

      DuckDBEngine.query(stream, sql, @config) do |row|
        row_queue.push(row)
      end

      row_queue.close
      writer_thread.join
      raise writer_exception if writer_exception

    else
      puts "DEBUG: Direct I/O - preserving original structure" if $DEBUG
      write_output(stream, cli_config)
    end
  end

  private

  def read_input(cli_config)
    if cli_config.input_stream?
      puts "DEBUG: Reading from stdin" if $DEBUG
      Reader.auto('-', @config, headers: cli_config.headers)
    elsif File.exist?(cli_config.input)
      puts "DEBUG: Reading from file: #{cli_config.input}" if $DEBUG
      if File.size(cli_config.input) == 0
        warn "Warning: Input file is empty"
        return ColumnarStream.new(nil, nil, [].each, @config)
      end

      options = { headers: cli_config.headers }
      options[:sheet] = cli_config.sheet if cli_config.sheet
      options[:range] = cli_config.range if cli_config.range
      options[:record_path] = cli_config.record_path if cli_config.record_path

      Reader.auto(cli_config.input, @config, **options)
    else
      warn "Error: Input file '#{cli_config.input}' does not exist"
      exit 1
    end
  end

  def write_output(stream, cli_config)
    output_path = cli_config.output_stream? ? '-' : cli_config.output

    options = {
      output_headers: cli_config.output_headers,
      mode: cli_config.mode,
      trim_empty_columns: cli_config.trim
    }

    case output_path
    when /\.xlsx?$/i
      options[:sheet] = cli_config.sheet if cli_config.sheet
      options[:start_cell] = cli_config.start_cell if cli_config.start_cell
      puts "DEBUG: XLSX output options: #{options.inspect}" if $DEBUG
    when /\.xml$/i
      options[:root_tag] = 'root'
      options[:row_tag] = 'row'
    end

    Writer.auto(stream, output_path, **options)
  end

  def write_output_streaming(row_queue, cli_config)
    output_path = cli_config.output_stream? ? '-' : cli_config.output

    options = {
      output_headers: cli_config.output_headers,
      mode: cli_config.mode,
      trim_empty_columns: cli_config.trim
    }

    case output_path
    when /\.xlsx?$/i
      options[:sheet] = cli_config.sheet if cli_config.sheet
      options[:start_cell] = cli_config.start_cell if cli_config.start_cell
    when /\.xml$/i
      options[:root_tag] = 'root'
      options[:row_tag] = 'row'
    end

    mock_stream = Object.new
    def mock_stream.each(&block)
      while !@queue.closed? || !@queue.empty?
        begin
          row = @queue.pop(true)
          block.call(row)
        rescue ThreadError
          break
        end
      end
    end
    mock_stream.instance_variable_set(:@queue, row_queue)
    mock_stream.define_singleton_method(:columns) { @config.get(:source_columns) }
    mock_stream.define_singleton_method(:config) { @config }

    Writer.auto(mock_stream, output_path, **options)
  end
end

module CCV
  module_function

  def run(args = ARGV)
    if args.empty? && $stdin.tty?
      Parser.show_help
      exit 0
    end

    pipeline = Pipeline.new
    pipeline.run(args)
  rescue => e
    warn "Error: #{e.message}"
    warn e.backtrace if $DEBUG
    exit 1
  end
end

CCV.run if __FILE__ == $PROGRAM_NAME
