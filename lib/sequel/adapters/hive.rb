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

      private

      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end
    end
  end
end
