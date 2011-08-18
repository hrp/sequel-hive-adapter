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
      
      def schema(table, opts={})
        hero = execute("DESCRIBE #{table}")
        hero[2..-1].map do |h|
          [h.first.strip.to_sym, {:db_type => h[1].strip.to_sym, :type => h[1].strip.to_sym}]
        end.reject{|r| [:"", :col_name].include?(r.first) || r.first[/Partition Information/] }
      end

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
          begin
            yield result
            #  width = result.first.size
            #  result.each do |r|
              #  row = {}
              #  r.each_with_index do |v, i| 
                #  row[columns[i]] = v.send(convert_type(columns[i]))
              #  end
              #  yield row
            #  end
          ensure
            #  result.close
          end
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
