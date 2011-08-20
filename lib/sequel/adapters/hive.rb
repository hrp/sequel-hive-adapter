require 'rbhive'

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

      private

      def disconnect_connection(c)
        c.close
      end
    end
    
    class Dataset < Sequel::Dataset
      SELECT_CLAUSE_METHODS = clause_methods(:select, %w'distinct columns from join where group having compounds order limit')

      #TODO better type conversion
      CONVERT_FROM = { :boolean => :to_s, :string => :to_s, :bigint => :to_i, :float => :to_f, :double => :to_f, :int => :to_i, :smallint => :to_i, :tinyint => :to_i }

      def schema
        @schema ||= @db.schema(@opts[:from].first)
      end

      def columns
        return @columns if @columns
        #  self.column_names
        p needs_schema_check?
        if needs_schema_check?
          @columns = schema.map{|c| c.first.to_sym}
        else
          @columns = @opts[:select].map do |col|
            col.respond_to?(:aliaz) ? col.aliaz : col
          end
        end
      end

      # Returns the function symbol that converts a column to the correct datatype
      def convert_type(column)
        return column
        return :to_s unless needs_schema_check?
        db_type = schema.select do |a|
          a.first == column
        end.flatten!
        CONVERT_FROM[db_type.last[:db_type]]
      end

      def fetch_rows(sql)
        execute(sql) do |result|
          result.each do |r|
            yield r
          end
          result.close
        end
        self
      end

      private

      def needs_schema_check?
        @opts[:select].nil? || @opts[:select].include?(:*)
      end

      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end
    end
  end
end
