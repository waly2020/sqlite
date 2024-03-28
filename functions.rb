def table_formater(table_name)
  table = {}
  columns = nil
  File.foreach(table_name).with_index do |line, key|
    if key.zero?
      columns = line.chomp.split(',').map(&:to_sym)
      columns << :id
    else
      row = {}
      CSV.parse_line(line).each_with_index { |val, i| row[columns[i]] = val }
      row[:id] = key
      table[key] = row
    end
  end
  return [table, columns]
end

def errors(table_name,query_type,order,joins,values,set,wheres)
  if table_name.nil?
    raise 'Must have a table!'
  end
  if query_type.nil?
    raise 'Must have a query type!'
  end
  if query_type != :select && (order.any? || joins.any?)
    raise 'Order and join can only be used with select!'
  end
  if query_type == :insert && values.nil?
    raise 'Insert must have values!'
  end
  if query_type == :update && set.nil?
    raise 'Update must have set!'
  end
  if query_type == :insert && !wheres.empty?
    raise "Insert can't have where!"
  end
  if query_type == :select && (values || set)
    raise "Select can't have values or set"
  end
end

def merge_colums(table,col_1, col_2, tab_1)
  new_table = {}
  i = 1
  table.each do |row_id, _v|
    tab_1.each do |rowb_id, _v|
      if table[row_id][col_1] == tab_1[rowb_id][col_2]
        new_table[i] = table[row_id].merge(tab_1[rowb_id])
        new_table[i][:id] = i
        i += 1
      end
    end
  end
  return new_table
end

def sort_column(col_a, col_b, order)
  if order == :ASC
    col_a <=> col_b
  else
    col_b <=> col_a
  end
end

def sort_table(table,main_order)

  if main_order.empty?
    return table
  end

  table.sort do |a, b|
    res = 0
    main_order.each do |order, column|
      res = sort_column(a[column], b[column], order)
      break unless res.zero?
    end
    res
  end

end

def check_where(wheres,row)
  wheres.all? do |where|
    if where[1].is_a? Array
      where[1].include?(row[where[0]])
    else
      row[where[0]] == where[1]
    end
  end
end

def select_where(table,wheres)

  if wheres.empty?
    return table
  end

  table.select do |_, row|
    check_where(wheres,row)
  end

end

def start_request_select(table,wheres,order,select)
  filtered = select_where(table,wheres)
  if select.include?(:*)
    selected = filtered.map { |_id, row| row }
  else
    selected = filtered.map { |_id, row| row.select { |k, _v| @select.include?(k) } }
  end
  sort_table(selected,order)
end

def twoFormsError(table_name)
  raise "Can't have two FROMs" if @table_name
end

def valueErros(values,set)
  raise "Can't have multiple values" if values
  raise "Can't have value and set" if set
end
