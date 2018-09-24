require 'sqlite3'

module Selection
  def find(*ids)

    if ids.length == 1
      find_one(ids.first)
    else
      ids.each do |id|
        return "Error: One or more ids are invalid" if invalid_id?(id)
      end
      rows = connection.execute <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        WHERE id IN (#{ids.join(",")});
      SQL

      rows_to_array(rows)
    end
  end

  def find_one(id)
    return "Error: invalid id" if invalid_id?(id)
    row = connection.get_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE id = #{id}
    SQL

    init_object_from_row(row)
  end

  def find_by(attribute, value)
    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
    SQL

    rows_to_array(rows)
  end

  def find_each(start=1, batch_size="ALL")
    return "Error: Please enter a number for start" if !start.is_a? Integer
    return "Error: Invalid batch size" if !batch_size.is_a? Integer || batch_size != "ALL"

    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      LIMIT #{batch_size} OFFSET #{start-1};
    SQL
    rows_to_array(rows)
  end

  def find_in_batches
  end

  def take(num=1)
    unless num.is_a? Integer return "Error: Please enter a number"
    if num > 1
      rows = connection.execute <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        ORDER BY random()
        LIMIT #{num}
      SQL

      rows_to_array(rows)
    else
      take_one
    end
  end

  def take_one
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY random()
      LIMIT 1;
    SQL

    init_object_from_row(row)
  end

  def first
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY id ASC LIMIT 1;
    SQL

    init_object_from_row(row)
  end

  def last
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY id DESC LIMIT 1;
    SQL

    init_object_from_row(row)
  end

  def all
    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table};
    SQL

    rows_to_array(rows)
  end

  def method_missing(method, *args)
    find_by(extracted_attribut(method), args)
  end


  private

  def invalid_id?(id)
    return false if id.is_a? Integer && id >= 0
    true
  end

  def extracted_attribute(method)
    method_parts = method.split('_')
    attribute = method_parts[2]
    attribute
  end

  def init_object_from_row(row)
    if row
      data = Hash[columns.zip(row)]
      new(data)
    end
  end


  def rows_to_array(rows)
    rows.map { |row| new(Hash[columns.zip(row)]) }
  end


end
