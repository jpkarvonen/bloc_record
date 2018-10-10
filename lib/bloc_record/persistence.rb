require 'sqlite3'
require_relative 'schema'

module Persistence
  def self.included(base)
    base.extend(ClassMethods)
  end

  def save
    self.save! rescue false
  end

  def save!
    unless self.id
      self.id = self.class.create(BlocRecord::Utility.instance_variable_to_hash(self)).id
      BlocRecord::Utility.reload_obj(self)
      return true
    end

    fields = self.class.attributes.map { |col| "#{col}=#{BlocRecord::Utility.sql_strings(self.instance_variable_get("@#{col}"))}" }.join(",")

    self.class.connection.execute <<-SQL
      UPDATE #{self.class.table}
      SET #{fields}
      WHERE id = #{self.id};
    SQL

    true
  end

  def update_attribute(attribute, value)
    self.class.update(self.id, { attribute => value })
  end

  def update_attributes(updates)
    self.class.update(self.id, updates)
  end

  module ClassMethods
     def create(attrs)
       attrs = BlocRecord::Utility.convert_keys(attrs)
       attrs.delete "id"
       vals = attributes.map { |key| BlocRecord::Utility.sql_strings(attrs[key]) }

       connection.execute <<-SQL
         INSERT INTO #{table} (#{attributes.join ","})
         VALUES (#{vals.join ","});
       SQL

       data = Hash[attributes.zip attrs.values]
       data["id"] = connection.execute("SELECT last_insert_rowid();")[0][0]
       new(data)
     end

     def update(ids, updates)
       if updates.class = Array
         updates.map! { |update|
           update = BlocRecord::Utility.convert_keys(update)
           update.delete "id"
           update = update.map { |key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}" }
         }

         0.upto(ids.length - 1) do |i|
           where_clause = "WHERE id = #{ids[i]};"
           connection.execute <<-SQL
             UPDATE #{table}
             SET #{updates[i].join("")} #{where_clause}
            SQL
         end
      else
        updates = BlocRecord::Utility.convert_keys(updates)
        updates.delete "id"
        update_array = updates.map { |key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}" }


        if ids.class == Fixnum
          where_clause = "WHERE id = #{ids};"
        elsif ids.class == Array
          where_clause = ids.empty? ? ";" : "WHERE id IN (#{ids.join(",")});"
        else
          where_clause = ";"
        end

        connection.execute <<-SQL
          UPDATE #{table}
          SET #{updates_array * ","} #{where_clause}
        SQL
      end

      true
    end

     def update_all(updates)
       update(nil, updates)
     end
   end
end
