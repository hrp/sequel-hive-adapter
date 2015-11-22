require 'rbhive'
require 'tempfile'

module Sequel
  module Hive
    class Database < Sequel::Database
      set_adapter_scheme :hive
      self.quote_identifiers = false
      self.identifier_input_method = :downcase
      
      def connect(server)
        opts = server_opts(server)
        RBHive::Connection.new(opts[:host], opts[:port] || 10_000)
      end

      def column_definition_primary_key_sql(sql, column)
        "" # noop        
      end

      def type_literal_generic_string(column)
        "string"
      end
      
      def dataset(opts = nil)
        Hive::Dataset.new(self, opts)
      end
    
      def execute(sql, opts={})
        synchronize(opts[:server]) do |conn|
          conn.open
          r = log_yield(sql){conn.fetch(sql)}
          yield(r) if block_given?
          r
        end
      end
      alias_method :do, :execute
      
      #
      # Returns the schema for the given table as an array with all members being arrays of length 2, 
      # the first member being the column name, and the second member being a hash of column 
      # information.
      #
      def schema(table, opts={})
        hero = execute("DESCRIBE #{table}")
        hero.map do |h|
          [ h[:col_name].to_sym, { :db_type => h[:data_type] , :comment => h[:comment] } ]
        end
      end

      #
      # Returns a list of tables as symbols.
      #
      def tables(opts={})
        execute('SHOW TABLES').map{|i| i.values}.reduce(:+).map{|i| i.to_sym}
      end

      def table_exists?(name)
        sch, table_name = schema_and_table(name)
        name = SQL::QualifiedIdentifier.new(sch, table_name) if sch

        !execute("SHOW TABLES '#{name}'").empty?
      end

      private

      def disconnect_connection(c)
        c.close
      end
    end
    
    class Dataset < Sequel::Dataset
      SELECT_CLAUSE_METHODS = clause_methods(:select, %w'distinct select columns from join where group having compounds order limit')

      def schema
        @schema ||= @db.schema(@opts[:from].first)
      end

      def columns
        @columns ||= schema.map{|c| c.first.to_sym}
      end

      def fetch_rows(sql)
        execute(sql) do |result|
          result.each do |r|
            yield r
          end
        end
        self
      end

      def insert_into_sql(sql)
        sql << " INTO TABLE "

        source_list_append(sql, @opts[:from])
      end

      def insert_columns_sql(sql)
        if is_load?
          sql
        else
          super(sql)
        end 
      end

      def insert_insert_sql(sql)
        if is_load?
          file = Tempfile.new('sequel-hive-')
  
          begin
            values = opts[:values]

            values.each do |v|
              file << "#{v}\001"
            end

            file << "\n"
          ensure
            file.close
          end

          sql << "LOAD DATA LOCAL INPATH '#{file.path}' "
        else
          super(sql)
        end
      end

      def insert_values_sql(sql)
        if is_load?
          sql
        else
          super(sql)
        end
      end

      def is_load?
        values = opts[:values]

        case values
        when Dataset
          false
        else
          true
        end
      end

      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end
    end
  end
end
