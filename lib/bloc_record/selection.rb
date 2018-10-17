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
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE id = #{id}
    SQL

    init_object_from_row(row)
  end

  def find_by(attribute, value)
    return "Error: invalid attribute" unless columns.include?(attribute)
    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
    SQL

    rows_to_array(rows)
  end

  def find_each(start=1, batch_size=100)
    return "Error: Please enter a number for start, 1 or higher" unless start.is_a? Integer && start > 0
    return "Error: Invalid batch size" unless batch_size.is_a? Integer

    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      LIMIT #{batch_size} OFFSET #{start-1};
    SQL
    batch_array = rows_to_array(rows)
    0.upto(batch_array.length -1) do |index|
      yield batch_array[index]
    end
  end

  def find_in_batches(start=1, batch_size=100)
    return "Error: Please enter a number for start, 1 or higher" unless start.is_a? Integer && start > 0
    return "Error: Invalid batch size" unless batch_size.is_a? Integer

    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      LIMIT #{batch_size} OFFSET #{start-1};
    SQL
    batch = rows_to_array(rows)
    yield(batch)
  end

  def take(num=1)
    return "Error: Please enter a number" unless num.class == Integer
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
    find_by(extract_attribute(method), args)
  end

  def where(*args)
    if args.count > 1
      expression = args.shift
      params = args
    else
      case args.first
      when String
        expression = args.first
      when Hash
        expression_hash = BlocRecord::Utility.convert_keys(args.first)
        expression = expression_hash.map {|key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}"}.join(" and ")
      end
    end

    sql = <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE #{expression};
    SQL

    rows = connection.execute(sql, params)

    rows_to_array(rows)

  end

  def where_not(*args)
    if args.count > 1
      expression = args.shift
      params = args
    else
      case args.first
      when String
        expression = args.first
      when Hash
        expression_hash = BlocRecord::Utility.convert_keys(args.first)
        expression = expression_hash.map {|key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}"}.join(" and ")
      end
    end

    sql = <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE NOT #{expression};
    SQL

    rows = connection.execute(sql, params)
    rows_to_array(rows)
  end

  def order(*args)
    if args.count > 1
      order = []
      args.each do |arg|
        if arg.is_a? Hash
         order << arg.map {|key, value| "#{key} #{value}"}.join(", ")
        else
         order << arg.to_s
        end
      end
      order = order.join(", ")
    elsif args.first.is_a? Hash
      order = args.first.map {|key, value| "#{key} #{value}"}.join(", ")
    else
      order = args.first.to_s
    end

    rows = connection.execute <<-SQL
      SELECT * FROM #{table}
      ORDER BY #{order};
    SQL
    rows_to_array(rows)
  end

  def join(*args)
    if args.count > 1
      joins = args.map { |arg| "INNER JOIN #{arg} ON #{arg}.#{table}_id = #{table}.id"}.join(" ")
      rows = connection.execute <<-SQL
        SELECT * FROM #{table} #{joins}
      SQL
    else
      case args.first
      when String
        rows = connection.execute <<-SQL
          SELECT * FROM #{table} #{BlocRecord::Utility.sql_strings(args.first)};
        SQL
      when Symbol
        rows = connection.execute <<-SQL
          SELECT * FROM #{table}
          INNER JOIN #{args.first} ON #{args.first}.#{table}_id = #{table}.id
        SQL
      when Hash
        joins = args.first.map {|key, value| "INNER JOIN #{key} ON #{key}.#{table}_id = #{table}.id INNER JOIN #{value} ON #{value}.#{table}_id = #{table}.id"}.join(" ")
        rows = connection.execute <<-SQL
          SELECT * FROM #{table} #{joins}
        SQL
      end
    end

    rows_to_array(rows)
  end

  private

  def invalid_id?(id)
    if (id.is_a? Integer) && id >= 0
      return false
    end
    true
  end

  def extract_attribute(method)
    method.to_s.split('_', 3)[2]
  end

  def init_object_from_row(row)
    if row
      data = Hash[columns.zip(row)]
      new(data)
    end
  end


  def rows_to_array(rows)
    collection = BlocRecord::Collection.new
    rows.each { |row| collection << new(Hash[columns.zip(row)]) }
    collection
  end
end
