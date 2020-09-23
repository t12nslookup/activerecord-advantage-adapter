#====================================================
#
#    Copyright 2008-2010 iAnywhere Solutions, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
#
#====================================================

require "active_record/connection_adapters/abstract_adapter"
require "arel/visitors/advantage.rb"

# Singleton class to hold a valid instance of the AdvantageInterface across all connections
class ADS
  include Singleton
  attr_accessor :api

  def initialize
    require "advantage" unless defined? Advantage
    @api = Advantage::AdvantageInterface.new()
    raise LoadError, "Could not load ACE library" if Advantage::API.ads_initialize_interface(@api) == 0
    raise LoadError, "Could not initialize ACE library" if @api.ads_init() == 0
  end
end

module ActiveRecord
  class Base
    DEFAULT_CONFIG = { :username => "adssys", :password => nil }
    # Main connection function to Advantage
    # Connection Adapter takes four parameters:
    # * :database (required, no default). Corresponds to "Data Source=" in connection string
    # * :username (optional, default to 'adssys'). Correspons to "User ID=" in connection string
    # * :password (optional, deafult to '')
    # * :options (optional, defaults to ''). Corresponds to any additional options in connection string

    def self.advantage_connection(config)
      config = DEFAULT_CONFIG.merge(config)

      raise ArgumentError, "No data source was given. Please add a :database option." unless config.has_key?(:database)

      connection_string = "data source=#{config[:database]};User ID=#{config[:username]};"
      connection_string += "Password=#{config[:password]};" unless config[:password].nil?
      connection_string += "#{config[:options]};" unless config[:options].nil?
      connection_string += "DateFormat=YYYY-MM-DD;"

      db = ADS.instance.api.ads_new_connection()

      ConnectionAdapters::AdvantageAdapter.new(db, logger, connection_string)
    end
  end

  module ConnectionAdapters
    class AdvantageException < StandardError
      attr_reader :errno
      attr_reader :sql

      def initialize(message, errno, sql)
        super(message)
        @errno = errno
        @sql = sql
      end
    end

    class AdvantageColumn < Column
      private

      # Overridden to handle Advantage integer, varchar, binary, and timestamp types
      def simplified_type(field_type)
        case field_type
        when /logical/i
          :boolean
        when /varchar/i, /char/i, /memo/i
          :string
        when /long binary/i
          :binary
        when /timestamp/i
          :datetime
        when /short|integer/i, /autoinc/i
          :integer
        else
          super
        end
      end

      # JAD Is this helpful?
      def initialize_type_map(m = type_map)
        m.register_type "logical", Type::Boolean.new
        m.register_type "varchar", Type::Text.new
        m.alias_type "char", "varchar"
        m.alias_type "memo", "varchar"
        m.register_type "long binary", Type::Binary.new
        m.register_type "integer", Type::Integer.new(limit: 5)
        m.alias_type "short", "integer"
        m.register_type "autoinc", Type::Integer.new(limit: 5)
        super
      end
    end

    class AdvantageAdapter < AbstractAdapter
      def initialize(connection, logger, connection_string = "") #:nodoc:
        super(connection, logger)
        @prepared_statements = false
        @auto_commit = true
        @affected_rows = 0
        @connection_string = connection_string
        @visitor = Arel::Visitors::Advantage.new self
        connect!
      end

      def adapter_name #:nodoc:
        "Advantage"
      end

      def supports_migrations? #:nodoc:
        true
      end

      def requires_reloading? #:nodoc:
        true
      end

      def active? #:nodoc:
        ADS.instance.api.ads_execute_immediate(@connection, "SELECT 1 FROM SYSTEM.IOTA") == 1
      rescue
        false
      end

      def disconnect! #:nodoc:
        result = ADS.instance.api.ads_disconnect(@connection)
        super
      end

      def reconnect! #:nodoc:
        disconnect!
        connect!
      end

      def supports_count_distinct? #:nodoc:
        true
      end

      def supports_autoincrement? #:nodoc:
        true
      end

      # Used from StackOverflow question 1000688
      # Strip alone will return NIL if the string is not altered.  In that case,
      # still return the string.
      def strip_or_self(str) #:nodoc:
        str.strip! || str if str
      end

      # Maps native ActiveRecord/Ruby types into ADS types
      def native_database_types #:nodoc:
        {
          :primary_key => "AUTOINC PRIMARY KEY CONSTRAINT NOT NULL",
          :string => { :name => "varchar", :limit => 255 },
          :text => { :name => "memo" },
          :integer => { :name => "integer" },
          :float => { :name => "float" },
          :decimal => { :name => "numeric" },
          :datetime => { :name => "timestamp" },
          :timestamp => { :name => "timestamp" },
          :time => { :name => "time" },
          :date => { :name => "date" },
          :binary => { :name => "blob" },
          :boolean => { :name => "logical" },
        }
      end

      # Applies quotations around column names in generated queries
      def quote_column_name(name) #:nodoc:
        %Q("#{name}")
      end

      def quoted_true #:nodoc:
        "1"
      end

      def quoted_false #:nodoc:
        "0"
      end

      # Translate the exception if possible
      def translate_exception(exception, message) #:nodoc:
        return super unless exception.respond_to?(:errno)
        case exception.errno
        when 2121
          if exception.sql !~ /^SELECT/i
            raise ActiveRecord::ActiveRecordError.new(message)
          else
            super
          end
        when 7076
          raise InvalidForeignKey.new(message, exception)
        when 7057
          raise RecordNotUnique.new(message, exception)
        else
          super
        end
        super
      end

      # The database update function.
      def update_sql(sql, name = nil) #:nodoc:
        execute(sql, name)
        return @affected_rows
      end

      # The database delete function.
      def delete_sql(sql, name = nil) #:nodoc:
        execute(sql, name)
        return @affected_rows
      end

      # The database insert function.
      # ActiveRecord requires that insert_sql returns the primary key of the row just inserted. In most cases, this can be accomplished
      # by immediatly querying the @@identity property. If the @@identity property is 0, then passed id_value is used
      def insert_sql(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil) #:nodoc:
        execute(sql, name)
        identity = last_inserted_id(nil)
        retval = id_value if retval == 0
        return retval
      end

      # The Database insert function as part of the rails changes
      def exec_insert(sql, name = nil, binds = []) #:nodoc:
        log(sql, "insert", binds) { exec_query(sql, binds) }
      end

      # The Database update function as part of the rails changes
      def exec_update(sql, name = nil, binds = []) #:nodoc:
        log(sql, "update", binds) { exec_query(sql, binds) }
      end

      # The Database delete function as part of the rails changes
      def exec_delete(sql, name = nil, binds = []) #:nodoc:
        log(sql, "delete", binds) { exec_query(sql, binds) }
      end

      def exec_query(sql, name = "SQL", binds = [])
        cols, record = execute(sql, name)
        ActiveRecord::Result.new(cols, record)
      end

      # Retrieve the last AutoInc ID
      def last_inserted_id(result) #:nodoc:
        rs = ADS.instance.api.ads_execute_direct(@connection, "SELECT LASTAUTOINC( CONNECTION ) FROM SYSTEM.IOTA")
        raise ActiveRecord::StatementInvalid.new("#{ADS.instance.api.ads_error(@connection)}:#{sql}") if rs.nil?
        ADS.instance.api.ads_fetch_next(rs)
        retval, identity = ADS.instance.api.ads_get_column(rs, 0)
        ADS.instance.api.ads_free_stmt(rs)
        identity
      end

      # Returns a query as an array of arrays
      def select_rows(sql, name = nil)
        exec_query(sql, name).rows
        #        rs = ADS.instance.api.ads_execute_direct(@connection, sql)
        #        raise ActiveRecord::StatementInvalid.new("#{ADS.instance.api.ads_error(@connection)}:#{sql}") if rs.nil?
        #        record = []
        #        while ADS.instance.api.ads_fetch_next(rs) == 1
        #          max_cols = ADS.instance.api.ads_num_cols(rs)
        #          result = Array.new(max_cols)
        #          max_cols.times do |cols|
        #            result[cols] = ADS.instance.api.ads_get_column(rs, cols)[1]
        #          end
        #          record << result
        #        end
        #        ADS.instance.api.ads_free_stmt(rs)
        #        return record
      end

      # Begin a transaction
      def begin_db_transaction #:nodoc:
        ADS.instance.api.AdsBeginTransaction(@connection)
        @auto_commit = false
      end

      # Commit the transaction
      def commit_db_transaction #:nodoc:
        ADS.instance.api.ads_commit(@connection)
        @auto_commit = true
      end

      # Rollback the transaction
      def rollback_db_transaction #:nodoc:
        ADS.instance.api.ads_rollback(@connection)
        @auto_commit = true
      end

      def add_lock!(sql, options) #:nodoc:
        sql
      end

      # Advantage does not support sizing of integers based on the sytax INTEGER(size).
      def type_to_sql(type, limit = nil, precision = nil, scale = nil) #:nodoc:
        if native = native_database_types[type]
          if type == :integer
            column_type_sql = "integer"
          elsif type == :string and !limit.nil?
            "varchar (#{limit})"
          else
            super(type, limit, precision, scale)
          end
        else
          super(type, limit, precision, scale)
        end
      end

      # Retrieve a list of Tables
      def data_source_sql(name = nil, type: nil) #:nodoc:
        "SELECT table_name from (EXECUTE PROCEDURE sp_GetTables( NULL, NULL, 'DISCOUNT', 'TABLE' )) spgc where table_cat <> 'system';"
      end

      # Retrieve a list of Tables
      def tables(name = nil) #:nodoc:
        sql = "EXECUTE PROCEDURE sp_GetTables( NULL, NULL, NULL, 'TABLE' );"
        select(sql, name).map { |row| strip_or_self(row["TABLE_NAME"]) }
      end

      # Return a list of columns
      def columns(table_name, name = nil) #:nodoc:
        table_structure(table_name).map do |field|
          AdvantageColumn.new(strip_or_self(field["COLUMN_NAME"]),
                              field["COLUMN_DEF"],
                              SqlTypeMetadata.new(sql_type: strip_or_self(field["TYPE_NAME"])),
                              field["NULLABLE"])
        end
      end

      # Return a list of indexes
      # EJS - Is there a way to get info without DD?
      def indexes(table_name, name = nil) #:nodoc:
        sql = "SELECT name, INDEX_OPTIONS & 1 AS [unique], index_expression FROM SYSTEM.INDEXES WHERE parent = '#{table_name}'"
        select(sql, name).map do |row|
          index = IndexDefinition.new(table_name, row["name"])
          index.unique = row["unique"] == 1
          index.columns = row["index_expression"]
          index
        end
      end

      # Return the primary key
      def primary_key(table_name) #:nodoc:
        sql = "SELECT COLUMN_NAME FROM (EXECUTE PROCEDURE sp_GetBestRowIdentifier( NULL, NULL, '#{table_name}', NULL, FALSE)) as gbri"
        rs = select(sql)
        if !rs.nil? and !rs[0].nil?
          strip_or_self(rs[0]["COLUMN_NAME"])
        else
          nil
        end
      end

      # Drop an index
      def remove_index(table_name, options = {}) #:nodoc:
        execute "DROP INDEX #{quote_table_name(table_name)}.#{quote_column_name(index_name(table_name, options))}"
      end

      # Rename a table
      #EJS - can be done without dd?
      def rename_table(name, new_name) #:nodoc:
        execute "EXECUTE PROCEDURE sp_RenameDDObject(#{quote_table_name(name)} , #{quote_table_name(new_name)}, 1 /* ADS_DD_TABLE_OBJECT */, 0 /* Rename File */)"
      end

      # Helper function to retrieve the columns current type
      def get_column_type(table_name, column_name) #:nodoc:
        sql = <<-SQL
SELECT
    CASE
    WHEN type_name = 'VARCHAR' or type_name = 'CHAR' or type_name = 'CICHAR' or
         type_name = 'NVARCHAR' or type_name = 'NCHAR' or type_name = 'VARBINARY'
         THEN CAST(TRIM(type_name) + '(' + TRIM(CAST(column_size AS SQL_CHAR)) + ')' AS SQL_CHAR)
    WHEN type_name = 'NUMERIC' and decimal_digits = 0
         THEN CAST('INTEGER(' + TRIM(CAST(column_size AS SQL_CHAR)) + ')' AS SQL_CHAR)
    WHEN type_name = 'NUMERIC' or type_name = 'DOUBLE' or type_name = 'CURDOUBLE'
         THEN CAST(TRIM(type_name) + '(' + TRIM(CAST(column_size AS SQL_CHAR)) + ',' + TRIM(CAST(decimal_digits AS SQL_CHAR)) + ')' AS SQL_CHAR)
    ELSE
         TRIM(type_name COLLATE ads_default_cs)
    END  AS "domain"
from (EXECUTE PROCEDURE sp_GetColumns( NULL, NULL, '#{table_name}', NULL)) as spgc
WHERE COLUMN_NAME = '#{column_name}'
SQL
        rs = select(sql)
        if !rs.nil? and !rs[0].nil?
          rs[0]["domain"]
        end
      end

      # Change a columns defaults.
      def change_column_default(table_name, column_name, default) #:nodoc:
        execute "ALTER TABLE #{quote_table_name(table_name)} ALTER #{quote_column_name(column_name)} #{quote_column_name(column_name)} #{get_column_type(table_name, column_name)} DEFAULT #{quote(default)}"
      end

      # Change a columns nullability
      def change_column_null(table_name, column_name, null, default = nil) #:nodoc:
        unless null || default.nil?
          execute("UPDATE #{quote_table_name(table_name)} SET #{quote_column_name(column_name)}=#{quote(default)} WHERE #{quote_column_name(column_name)} IS NULL")
        end
        execute("ALTER TABLE #{quote_table_name(table_name)} ALTER #{quote_column_name(column_name)} #{quote_column_name(column_name)} #{get_column_type(table_name, column_name)} CONSTRAINT #{null ? "" : "NOT"} NULL")
      end

      # Alter a column
      def change_column(table_name, column_name, type, options = {}) #:nodoc:
        add_column_sql = "ALTER TABLE #{quote_table_name(table_name)} ALTER #{quote_column_name(column_name)} #{quote_column_name(column_name)} #{type_to_sql(type, type_options[:limit], type_options[:precision], type_options[:scale])}"
        add_column_options!(add_column_sql, options)
        execute(add_column_sql)
      end

      # Add column options
      def add_column_options!(sql, options) #:nodoc:
        sql << " DEFAULT #{quote(options[:default], options[:column])}" if options_include_default?(options)
        # must explicitly check for :null to allow change_column to work on migrations
        if options[:null] == false
          sql << " CONSTRAINT NOT NULL"
        end
      end

      # Rename a column
      def rename_column(table_name, column_name, new_column_name) #:nodoc:
        execute "ALTER TABLE #{quote_table_name(table_name)} ALTER #{quote_column_name(column_name)} #{quote_column_name(new_column_name)} #{type_to_sql(type, type_options[:limit], type_options[:precision], type_options[:scale])}"
      end

      # Drop a column from a table
      def remove_column(table_name, column_name) #:nodoc:
        execute "ALTER TABLE #{quote_table_name(table_name)} DROP #{quote_column_name(column_name)}"
      end

      protected

      # Execute a query
      def select(sql, name = nil, binds = []) #:nodoc:
        # return execute(sql, name, binds)
        exec_query(sql, name, binds)
      end

      # Queries the structure of a table including the columns names, defaults, type, and nullability
      # ActiveRecord uses the type to parse scale and precision information out of the types. As a result,
      # chars, varchars, binary, nchars, nvarchars must all be returned in the form <i>type</i>(<i>width</i>)
      # numeric and decimal must be returned in the form <i>type</i>(<i>width</i>, <i>scale</i>)
      # Nullability is returned as 0 (no nulls allowed) or 1 (nulls allowed)
      # Alos, ActiveRecord expects an autoincrement column to have default value of NULL
      def table_structure(table_name)
        sql = "SELECT COLUMN_NAME, IIF(COLUMN_DEF = 'NULL', null, COLUMN_DEF) as COLUMN_DEF, IIF(TYPE_NAME = 'NUMERIC' and DECIMAL_DIGITS = 0, 'INTEGER', TYPE_NAME) as TYPE_NAME, NULLABLE from (EXECUTE PROCEDURE sp_GetColumns( NULL, NULL, '#{table_name}', NULL )) spgc where table_cat <> 'system';"
        # sql = "SELECT COLUMN_NAME, IIF(COLUMN_DEF = 'NULL', null, COLUMN_DEF) as COLUMN_DEF, TYPE_NAME, NULLABLE from (EXECUTE PROCEDURE sp_GetColumns( NULL, NULL, '#{table_name}', NULL )) spgc where table_cat <> 'system';"
        structure = exec_query(sql, :skip_logging)
        raise(ActiveRecord::StatementInvalid, "Could not find table '#{table_name}'") if structure == false
        structure.to_ary
      end

      # Required to prevent DEFAULT NULL being added to primary keys
      def options_include_default?(options)
        options.include?(:default) && !(options[:null] == false && options[:default].nil?)
      end

      private

      # Connect
      def connect! #:nodoc:
        result = ADS.instance.api.ads_connect(@connection, @connection_string)
        if result != 1
          error = ADS.instance.api.ads_error(@connection)
          raise ActiveRecord::ActiveRecordError.new("#{error}: Cannot Establish Connection")
        end
      end

      # The database execution function
      def query(sql, name = nil, binds = []) #:nodoc:
        if name == :skip_logging
          execute(sql, binds)
        else
          log(sql, name, binds) { execute(sql, binds) }
        end
      end

      # Execute a query
      def execute(sql, name = nil, binds = []) #:nodoc:
        return if sql.nil?

        if binds.empty?
          rs = ADS.instance.api.ads_execute_direct(@connection, sql)
        else
          stmt = ADS.instance.api.ads_prepare(@connection, sql)
          # bind each of the parameters
          # col: Parameter array.  Col[0] -> Parameter info, Col[1] -> Parameter value
          binds.each_with_index { |col, index|
            result, param = ADS.instance.api.ads_describe_bind_param(stmt, index)
            if result == 1
              # For date/time/timestamp fix up the format to remove the timzone
              if (col[0].type === :datetime or col[0].type === :timestamp or col[0] === :time) and !col[1].nil?
                param.set_value(col[1].to_s(:db))
              else
                param.set_value(col[1])
              end
              ADS.instance.api.ads_bind_param(stmt, index, param)
            else
              result, errstr = ADS.instance.api.ads_error(@connection)
              raise AdvantageException.new(errstr, result, sql)
            end
          } #binds.each_with_index
          result = ADS.instance.api.ads_execute(stmt)
          if result == 1
            rs = stmt
          else
            result, errstr = ADS.instance.api.ads_error(@connection)
            raise AdvantageException.new(errstr, result, sql)
          end
        end
        if rs.nil?
          result, errstr = ADS.instance.api.ads_error(@connection)
          raise AdvantageException.new(errstr, result, sql)
        end

        row_record = []
        all_cols = []
        if (ADS.instance.api.ads_num_cols(rs) > 0)
          while ADS.instance.api.ads_fetch_next(rs) == 1
            max_cols = ADS.instance.api.ads_num_cols(rs)
            row = []
            max_cols.times do |cols|
              # result[ADS.instance.api.ads_get_column_info(rs, cols)[2]] = ADS.instance.api.ads_get_column(rs, cols)[1]
              if row_record.count == 0 then
                cinfo = ADS.instance.api.ads_get_column_info(rs, cols)
                all_cols << cinfo[2]
              end
              cvalue = ADS.instance.api.ads_get_column(rs, cols)
              row << cvalue[1]
            end
            row_record << row
          end
          @affected_rows = 0
        else
          @affected_rows = ADS.instance.api.ads_affected_rows(rs)
        end
        ADS.instance.api.ads_free_stmt(rs)

        all_cols.uniq!
        return all_cols, row_record
      end
    end
  end
end
