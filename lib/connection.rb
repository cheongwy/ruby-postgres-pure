require 'binary_converter'
require 'connection_parser'
require 'message_generator'
require 'auth_handler'
require 'result'
require 'error'
require 'char_set'
require 'socket'

module Pg
  
  class Connection
    include BinaryConverter
    include ConnectionParser
    include MessageGenerator
    include AuthHandler
    
    attr_reader :db, :user, :host, :port, :options
    
    #connection_str_or_hash_or_multi
    def initialize(*params)
      @parameter_status = {}
      @conn_params = params
      @copying = false
      @copy_data = []  
        
      return open(nil, nil, nil, nil, nil, nil, nil) if params.nil? or params.size == 0
        
      if params.size == 1
        if params[0].is_a?(String)
          p = parse_as_string(params[0])
          @conn_params = p
          open(p[0], p[1], p[2], p[3], p[4], p[5], p[6])
        elsif params[0].is_a?(Hash)
          p = parse_as_hash(params[0])
          @conn_params = p
          open(p[0], p[1], p[2], p[3], p[4], p[5], p[6])
        else
          raise ArgumentError.new("Invalid arguments")   
        end
      elsif params.size == 7
          open(params[0], params[1], params[2], params[3], params[4], params[5], params[6])
      else
        raise ArgumentError.new("Invalid arguments")
      end
      
    end
    
    #async_exec(sql [, params, result_format ] ) → PG::Result 
    #async_exec(sql [, params, result_format ] ) {|pg_result| block } 
    #This function has the same behavior as exec, except that it’s implemented using asynchronous command processing 
    #in order to allow other threads to process while waiting for the server to complete the request.
    
    #async_query(*args)
    #Alias for: async_exec
    
    #
    # Get the pid of the backend process
    #
    def backend_pid
      # This should not happen. pid should have been set in auth_handler - process_backend_key_data
      raise "No PID for backend" if @pid.nil? 
      @pid
    end
    
    #
    # 
    #
    def cancel
      raise "No process id and secret key available to perform cancellation" if @pid.nil? || @secret.nil?
      p = @conn_params
      begin
        conn = Pg::Connection.new(p[0], p[1], p[2], p[3], p[4], p[5], p[6])
        puts "Cancelling request with pid #{@pid} and secret #{@secret}"
        conn.cancel_message(@pid, @secret)
      ensure
        conn.close() unless conn.nil?
      end
    end
    
    # client_encoding=(p1)
    # Alias for: set_client_encoding
    
    def close
      unless @socket.nil? || closed?
        puts "Closing connection"
        @socket.write("X#{b_int32(4)}")
        @socket.close()
        @closed = true
      end   
    end
    
    def closed?
      @closed == true
    end
    
    def describe_portal(portal_name)
      describe_portal_message(portal_name)
      write(sync_message())
      process_query_response()
    end
    
    def describe_prepared(stmt_name)
      write(describe_prepared_message(stmt_name))
      write(sync_message())
      process_query_response()
    end
    
    def error_message
      @error && @error.message
    end
    
    # TODO implement escape methods?
    
    #
    # Sends SQL query request specified by sql to PostgreSQL. 
    # Returns a PG::Result instance on success. On failure, it raises a PG::Error.
    #
    def exec(sql, &block)
      result = execute_query {
        write(plain_sql_message(sql))
        process_query_response()
      }
      block.nil? ? result : yield(result)
    end    
    alias_method :query, :exec  

    #
    # Execute prepared named statement specified by statement_name. 
    # Returns a PG::Result instance on success. On failure, it raises a PG::Error.
    #
    def exec_prepared(stmt_name, params = [], &block)
      result = exec_prepared_statement(stmt_name, params)
      block.nil? ? result : yield(result) 
    end
    
    def external_encoding
      CharSet.pg_to_ruby(@parameter_status['server_encoding'])
    end
    
    #
    # Close the connection
    #
    def finish
      close()
    end
    
    #
    # Query if the connection is closed/finished
    #
    def finished?
      closed?
    end
    
    def flush
      # How to implement this?
      # This feels more like flushing of the client socket
      # than sending the flush command to the server
      # Always return true for now
#      @socket.write(flush_message())
#      process_query_response()
      true
    end
    
    def get_client_encoding
      @client_encoding
    end
    
    def get_copy_data
      @copy_data.pop()
    end
    
    # get_result() → PG::Result 
    # get_result() {|pg_result| block } 
    
    # Returns:
    # an Encoding - client_encoding of the connection as a Ruby Encoding object.
    # nil - the client_encoding is ‘SQL_ASCII’
    def internal_encoding
      get_client_encoding unless @parameter_status['client_encoding'] == 'SQL_ASCII'
    end
    
    # internal_encoding = value 
    # A wrapper of set_client_encoding.
    
    def pass
      @user
    end
    
    def parameter_status(param_name)
      @parameter_status[param_name] unless @parameter_status.nil? 
    end
    
    def prepare(name, sql, param_type_oids=[])
      write(parse_message(name, sql, param_type_oids))
      write(sync_message())
      
      code = @socket.recv(1)
      puts "Parse response code #{code}"
      parse_error_response() if(code == 'E')
      
      len = @socket.recv(4)
      raise "Prepared Statement Parse Error" unless code.to_i == 1 # Code for parse complete is 1
      parse_status_params_and_wait_ready_for_query()
      Result.new({:fields => []})
    end
    
    def protocol_versio
      3
    end
    
    def put_copy_data(buffer)
      @socket.write(copy_data_message(buffer))
      true
    end
    
    def put_copy_end()
      @socket.write(copy_done_message())
      @copying = false
      parse_status_params_and_wait_ready_for_query()
      true
    end
    
    def reset
      close
      p = @conn_params
      open(p[0], p[1], p[2], p[3], p[4], p[5], p[6])
    end
    
    def server_version
      @parameter_status['server_version']
    end
    
    def set_client_encoding(encoding)
      begin
        @client_encoding = CharSet.pg_to_ruby(encoding)
      rescue ArgumentError => e
        raise Error.new('invalid encoding name')
      end
    end
    alias_method :client_encoding=, :set_client_encoding
    
    def set_default_encoding()
      default_enc = Encoding.default_internal
      @client_encoding = CharSet.ruby_to_pg(default_enc) unless default_enc.nil?
    end
    
    def transaction_status
      if @query_ready
        0
      else
        
      end
        
    end
    
    private
    
    def open(hostname, port, options, tty, dbname, user, password)
      
      # defaults
      hostname ||= '127.0.0.1'
      port ||= 5432
      user ||= ENV['USER'] || ENV['USERNAME']
      dbname ||= user
      
      @host = hostname
      @db = dbname
      @port = port  
      @user = user
      @options = options
      #@socket = UNIXSocket.new("/tmp/.s.PGSQL.5432")
      @closed = false
      @socket = TCPSocket.open(hostname, port)
      begin
        do_auth(user, password, dbname)
        @client_encoding = CharSet.pg_to_ruby(@parameter_status['client_encoding'])
      rescue Exception => e
        close()
        raise e
      end
      
    end
    
    def execute_query(&block)
      #puts "Executing query #{@query_ready}"
      if @query_ready || @copying
        @query_ready = false
        yield
      else
        #next_response()
        raise "Connection is not ready!! This should not happen!!"  
      end        
    end
    
    def exec_prepared_statement(stmt_name, params = [])
      execute_query {
        write(bind_message(stmt_name, params))
        #@socket.write(flush_message())
  #      code = @socket.recv(1)
  #      puts "Bind response code #{code}"      
        
        write(describe_portal_message())
        #puts "Executing query"
        write(execute_message())
        # only sync after sending execute msg  
        write(sync_message())
        
        code = @socket.recv(1)
        puts "Bind response code #{code}"
        len = @socket.recv(4)
        
        parse_error_response() if(code == 'E')
        raise "Prepared Statement Bind Error" unless code.to_i == 2
        
        code = @socket.recv(1)
        puts "Code after exec #{code}"
        case code
        when 'C'
          return command_completed()
        when 'T'
          return parse_row_description()
        when 'n'
          return no_data()        
        when 's'
          return portal_suspended_response()
        when 'E'
          return parse_error_response()
        else
          raise "Unknown/Not implemented response code of #{code}"
        end      
      }      
    end
    
    def process_query_response
      code = @socket.recv(1)
      puts "Query response #{code}"
      case code
      when 'C'
        return command_completed()
      # TO BE IMPLEMENTED
      # CopyInResponse
      when 'H'
        return parse_copy_out_response()
      when 'G'
        return parse_copy_in_response()
      when 'T'
        return parse_row_description()
      when 'I'
        return empty_query_response()
      when 'E'
        return parse_error_response()
      when 'Z'
        process_ready_for_query()          
      when 'N'
        return process_notice_response()
      when 'A'
        return process_notification_response()
      when 't'
        return parameter_description_response()  
      else
        close()
        raise "Unknown/Not implemented response code #{code}"
      end
      
    end
    
    def process_ready_for_query()
      length = int32(@socket.recv(4))
      status  = @socket.recv(1)
      raise "Unknown transaction status #{status}" unless ['I', 'T', 'E'].include?(status)
      puts "Transaction status #{status}"
      @query_ready = true
    end         
    
    def parse_error_response()

      len = int32(@socket.recv(4))
      err = @socket.recv(len)
      arr = err.split(/\x0/)
      query_ready_code = arr[arr.size-1]
      
      puts "Query error response #{err}"
      puts "Query ready code #{query_ready_code}"
      unless query_ready_code == 'Z'
        begin
          read = @socket.recv(0)
          puts "Read after error #{read}"
        rescue Exception => e
          raise Error.new(err)
        end
      end
      
      # Don't understand why we need to consume 2 more bytes to get the transaction status
      tstatus = @socket.recv(2)
      puts "Transaction status after error #{tstatus}"
      @query_ready = true
      @error = Error.new(arr)
      raise @error unless @copying
    end
    
    def process_notice_response
      len = int32(@socket.recv(4))
      notices = @socket.recv(len-4)
      arr = notices.split(/\x0/)
      puts arr.join(' ')
      process_query_response()
    end
    
    def process_notification_response()
      len = int32(@socket.recv(4))
      pid = int32(@socket.recv(4))
      channel_and_payload = @socket.recv(len-8)  
    end 
    
    def parse_status_params_and_wait_ready_for_query()
      code = @socket.recv(1)
      puts "Status param code #{code}"
      case code
      when 'C'
        command_completed()
      when 'T'
        parse_row_description()                
      when 'Z'
        process_ready_for_query()
      when 'N'  
        process_notice_response()
      when 'A'
        process_notification_response()        
      when 'I'
        empty_query_response()
      when 'E'
        parse_error_response()
      else
        raise "Unknown response code #{code}"
      end
    end
    
    def parse_row_description()
      length = int32(@socket.recv(4))
      fields = parse_field_descriptions()
      
      next_code = @socket.recv(1)
      puts "Next code for row #{next_code}"
      if(next_code == 'D')
        result = { :fields => fields, :rows => [] }
        return parse_data_row(result, 0)
      elsif(next_code == 'E')
        return parse_error_response()
      elsif(next_code == 'Z')
        return Result.new({ :fields => fields, :rows => [] })
      end
    end
    
    def parse_field_descriptions
      field_count = int16(@socket.recv(2))
      puts "Field count #{field_count}"
      fields = [] 
      (1..field_count).each { | c |
        field_name = ''
        while((b = @socket.recv(1)) != "\0")
          field_name += b
        end
        
        attributes = {
          :table_id => int32(@socket.recv(4)),
          :attr_num => int16(@socket.recv(2)),
          :data_type_id => int32(@socket.recv(4)),
          :data_type_size => int16(@socket.recv(2)),
          :type_modifier => int32(@socket.recv(4)),
          :format_code => int16(@socket.recv(2))
        }
        #puts "Name #{field_name} => #{attributes}"
          
        fields << { field_name => attributes }
      }      
      fields
    end
    
    def parse_data_row(result, row_count)
      length = int32(@socket.recv(4))
      values = int16(@socket.recv(2))
      
      rvalues = []
      (1..values).each { | i |
        val_len = int32(@socket.recv(4))
        val = @socket.recv(val_len)
        #puts "Got val #{val}"
        #field = result[:fields][i-1]  
        rvalues[i-1] = val
        result[:rows][row_count] = { :values => rvalues }
      }
      
      next_code = @socket.recv(1)
      #puts "Next code #{next_code}"
      if(next_code == 'C')
        status = command_completed(true)
        puts "Final row count #{row_count}"
        result[:cmd_status] = status
        result[:cmd_rows] = row_count+1
        result[:result_status] = Result::PGRES_TUPLES_OK
        #puts "result #{result[:rows]}"
        return Result.new(result)
      elsif(next_code == 'D') 
        row_count += 1
        return parse_data_row(result, row_count)
      end    
         
    end
    
    def command_completed(data=false)
      length = int32(@socket.recv(4))
      res = @socket.recv(length-4)
      #puts "Command completed #{res}"
      parse_status_params_and_wait_ready_for_query()
      return res if data
      get_empty_result(Result::PGRES_COMMAND_OK)
    end
    
    def parse_copy_out_response()
      parse_copy_response()
      
      @copy_data = []
      while (chunk = parse_copy_data()) != nil
        @copy_data << chunk
      end
      #puts "Copy data #{data}"
      parse_status_params_and_wait_ready_for_query()
      get_empty_result(Result::PGRES_COPY_OUT)
    end
    
    def parse_copy_in_response()
      parse_copy_response()
      @copying = true
      get_empty_result(Result::PGRES_COPY_IN)      
    end
    
    def parse_copy_response
      length = int32(@socket.recv(4))
      data_format_code = @socket.recv(1)
      col_count = int16(@socket.recv(2))
      col_codes = (0..col_count-1).collect { |cnt|
        int16(@socket.recv(2))
      }
      #puts "Copy data codes #{col_codes}"
    end
    
    def parse_copy_data()
      code = @socket.recv(1)
      #puts "Got copy code #{code}"
      
      raise "Copy code expected but got #{code}" unless code == 'd' || code == 'c'
      length = int32(@socket.recv(4))
      return nil if code == 'c'

      data = @socket.recv(length-4)
      data
    end
    
    def empty_query_response
      #puts "Got empty query"
      consume_empty_response_and_wait_ready_for_query()
      get_empty_result(Result::PGRES_EMPTY_QUERY)
    end
    
    def portal_suspended_response
      consume_empty_response_and_wait_ready_for_query()
    end
    
    def parameter_description_response
      length = int32(@socket.recv(4))
      param_count = int16(@socket.recv(2))
      if(param_count > 0)
        oids = [] 
        [0..param_count].each {
          oids.push(int32(@socket.recv(4)))
        }
      end
      
      code = @socket.recv(1)
      case code
      when 'n'
        no_data()
        return Result.new({ :fields => [], :rows => []})
      when 'T'
        return parse_row_description()  
      end
              
      raise "Unexpected code after parameter description #{code}"
    end
    
    def no_data
      #puts 'No data'
      consume_empty_response_and_wait_ready_for_query()
    end

    def consume_empty_response_and_wait_ready_for_query()
      @socket.recv(4)
      parse_status_params_and_wait_ready_for_query()
    end
    
    def get_empty_result(status)
      Result.new({ :fields => [], :rows => [], :result_status => status})
    end
    
    def write(msg)
      if @copying
        puts "Copying in progress. Failing it first before new write"
        fail_copy() 
      end
      @socket.write(msg)
    end
    
    def fail_copy
      @socket.write(copy_fail_message('COPY terminated by new PQexec'))
      parse_status_params_and_wait_ready_for_query()
      @copying = false
    end
    
#    def next_response
#      code = @socket.recv(1)
#      puts "Next #{code}"
#      case code
#      when 'Z'
#        process_ready_for_query()
#      when 'N'  
#        process_notice_response()
#      when 'I'
#        empty_query_response()
#      when 'T'
#        parse_row_description()                
#      when 'E'
#        parse_error_response()
#      else
#        raise "Unknown response code #{code}"
#      end      
#    end        
    
  end
  
  
end