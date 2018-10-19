require 'sqlite3'
require 'pg'

module Connection
  def connection
    if BlocRecord.database_platform == :sqlite3
      @connection ||= SQLite3::Database.new(BlocRecord.database_filename)
    elsif BlocRecord.database_platform == :pg
      @connection ||= PG::Connection.open(:dbname => BlocRecord.database_filename)
    end
  end
end
