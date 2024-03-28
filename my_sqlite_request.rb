require 'csv'
require_relative 'functions'

class MySqliteRequest
  ORDER = %w[DESC ASC].freeze

  attr_reader :query_type

  def initialize(table_name = nil)
    @table_name = table_name
    @table_name = @table_name.nil? || @table_name.end_with?('.csv') ? @table_name : @table_name << '.csv'
    @query_type = nil
    @joins = []
    @wheres = []
    @select = []
    @order = []
    @values = nil
    @set = nil
    @table = nil
    @columns = nil
    @id = nil
  end

  def run
    errors(@table_name,@query_type,@order,@joins,@values,@set,@wheres)
    @table, @columns = formate_table(@table_name)
    check_column
    join_tables
    case @query_type
    when :select
      start_request_select(@table,@wheres,@order,@select)
    when :insert
      start_request_insert
    when :update
      start_request_update
    when :delete
      start_request_delete
    end
  end

  def from(table_name)

    twoFormsError(table_name)

    if !@table_name
      @table_name = table_name
      @table_name.nil? || @table_name.end_with?('.csv') ? @table_name : @table_name << '.csv'
      self
    end

  end

  def select(*col_names)

    @query_type ||= :select
    check_query(:select)

    @select.concat(col_names.map(&:to_sym))
    self
  end

  def where(col_name, value)
    @wheres << [col_name.to_sym, value]
    self
  end

  def join(col_1, tab_1, col_2)
    if tab_1.nil? || tab_1.end_with?('.csv')
      @joins << [col_1.to_sym, tab_1, col_2.to_sym]
    else
      tab_1 << '.csv'
      @joins << [col_1.to_sym, tab_1, col_2.to_sym]
    end
    self
  end

  def order(column_name, order = nil)

    if order.nil?
      order = 'ASC'
    end

    unless ORDER.include?(order.upcase)
      raise 'Order must be ASC or DESC'
    end

    @order << [order.upcase.to_sym, column_name.to_sym]
    self
  end

  def insert(table_name)

    twoFormsError(table_name)

    @table_name = table_name
    @table_name.nil? || @table_name.end_with?('.csv') ? nil : @table_name << '.csv'
    @query_type ||= :insert
    check_query(:insert)
    self
  end

  def values(*datas)
    valueErros(@values,@set)
    @values = datas
    self
  end

  def update(table_name)
    twoFormsError(table_name)

    @table_name = table_name
    @table_name.nil? || @table_name.end_with?('.csv') ? nil : @table_name << '.csv'
    @query_type ||= :update
    check_query(:update)
    self
  end

  def set(data)
    valueErros(@values,@set)

    @set = data.transform_keys(&:to_sym)
    self
  end

  def delete
    @query_type ||= :delete
    check_query(:delete)
    self
  end

  def check_query(query_type)
    if @query_type != query_type
      raise "Can't have different query types"
    end
  end

  def check_column
    if @query_type == :insert && (@columns.size != @values.size + 1)
      raise "Insert column value mismatch!"
    end
  end

  def formate_table(table_name)
    table_format = table_formater(table_name)
    @id = table_format[0].size + 1
    table_format
  end

  def join_tables
    @joins.each do |join|
      col_1 = join[0]
      tab_1, columns_copy = formate_table(join[1])
      col_2 = join[2]

      @columns = (@columns + columns_copy).uniq
      @table = merge_colums(@table,col_1, col_2, tab_1)
    end
  end

  def start_request_insert
    new_row = {}
    @columns[0...-1].each.with_index { |col, i| new_row[col] = @values[i] }
    @table[@id] = new_row
    @id += 1
    init_database
  end

  def start_request_delete
    if @wheres.empty?
      @table = {}
    else
      delete_rows
    end
    init_database
  end

  def delete_rows
    @table.each do |id, row|
      if check_where(@wheres,row)
        @table.delete(id)
      end
    end
  end

  def start_request_update
    if @wheres.empty?
      @table.each { |_id, row| update_row(row) }
    else
      @table.select do |_, row|
        update_row(row) if check_where(@wheres,row)
      end
    end
    init_database
  end

  def update_row(row)
    @set.each { |key, value| row[key] = value }
  end

  def init_database
    CSV.open(@table_name, 'w') do |csv_table|
      csv_table << @columns[0...-1]
      @table.each { |_id, row| csv_table << row.reject { |k, _v| k == :id }.values }
    end
  end
end
