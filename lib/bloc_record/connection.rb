require 'sqlite3'
require 'pg'

module Connection
  def connection
    if BlocRecord.database_platform == :sqlite3
      @connection ||= SQLite3::Database.new(BlocRecord.database_filename)
    elsif BlocRecord.database_platform == :pg
      @connection ||= PG::Connection.new(:dbname => BlocRecord.database_filename)
    end
  end

  def execute(sql_string)
    if BlocRecord.database_platform == :sqlite3
      connection.execute(sql_string)
    elsif BlocRecord.database_platform == :pg
      connection.exec(sql_string)
    end
  end

 def get_first_row(sql_string)
   if BlocRecord.database_platform == :sqlite3
     connection.get_first_row(sql_string)
   elsif BlocRecord.database_platform == :pg
     connection.send_query(sql_string)
     connection.set_single_row_mode
     connection.get_result
   end
 end

end
