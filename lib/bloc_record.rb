module BlocRecord
  def self.connect_to(filename, database_platform)
    @database_filename = filename
    @database_platform = database_platform
  end

  def self.database_filename
    @database_filename
  end

  def self.database_platform
    @database_platform
  end
end
