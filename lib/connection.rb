require 'binary_converter'
require 'connection_parser'
require 'message_generator'
require 'result'
require 'error'
require 'digest/md5'

module Pg
  
  class Connection
    include BinaryConverter
    include ConnectionParser
    include MessageGenerator
    
    attr_reader :db, :user, :host, :port
    
    @@auth_types = { 
      :kerberos => { :length => 8, :code => 2 },
      :clear => { :length => 8, :code => 3 },
      :md5 => { :length => 12, :code => 5 }
    }    
    
    #connection_str_or_hash_or_multi
    def initialize(*params)
      @parameter_status = {}
        
      return open(nil, nil, nil, nil, nil, nil, nil) if params.nil? or params.size == 0
        
      if params.size == 1
        if params[0].is_a?(String)
          p = parse_as_string(params[0])
          open(p[0], p[1], p[2], p[3], p[4], p[5], p[6])
        elsif params[0].is_a?(Hash)
          p = parse_as_hash(params[0])
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
    
    #
    # Get the pid of the backend process
    #
    def backend_pid
      @pid
    end
    
    #
    #
    #
    def cancel
      raise "No process id and secret key available to perform cancellation" if @pid.nil? || @secret.nil?
    end
    
    def close
      unless @socket.nil?
        @socket.write("X#{b_int32(4)}")
        @socket.close()
        @closed = true
      end   
    end
    
    def closed?
      @close
    end
    
    #
    # Sends SQL query request specified by sql to backend
    #
    def exec(sql)
      execute_query {
        @socket.write(plain_sql_message(sql))
        
        code = @socket.recv(1)
        #puts "Query response #{code}"
        case code
        when 'T'
          return parse_row_description()
        when 'C'
          return command_completed()
        when 'I'
          return empty_query_response()
        when 'E'
          return parse_error_response()
        else
          raise "Unknown/Not implemented response code #{code}"
        end
        
      }
    end
    
    #
    #
    #
    def exec_prepared(stmt_name, params = [])
      execute_query {
        @socket.write(bind_message(stmt_name, params))
        #@socket.write(flush_message())
  #      code = @socket.recv(1)
  #      puts "Bind response code #{code}"      
        
        @socket.write(describe_portal())
        #puts "Executing query"
        @socket.write(execute_message())
        # only sync after sending execute msg  
        @socket.write(sync_message())
        
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
    
    def parameter_status(param_name)
      @parameter_status[param_name] unless @parameter_status.nil? 
    end
    
    def prepare(name, sql)
      @socket.write(parse_message(name, sql))
      @socket.write(sync_message())
      
      code = @socket.recv(1)
      puts "Parse response code #{code}"
      parse_error_response() if(code == 'E')
      
      len = @socket.recv(4)
      raise "Prepared Statement Parse Error" unless code.to_i == 1
      parse_status_params_and_wait_ready_for_query()
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
      #@socket = UNIXSocket.new("/tmp/.s.PGSQL.5432")
      @socket = TCPSocket.open(hostname, port)
      do_auth(user, password, dbname)
      
      @user = user
    end
    
    def do_auth(user, password, dbname)
      
      @socket.write(startup_message(user,dbname))
      auth_code = @socket.recv(1)
      
      if(auth_code == 'R')
        length = int32(@socket.recv(4))
        res_code = int32(@socket.recv(4))
        handle_auth(length, res_code, user, password)
        
        auth_response = @socket.recv(1)
        puts "Auth response #{auth_response}"
        if(auth_response == 'R')
          res_len = int32(@socket.recv(4))
          res_code = int32(@socket.recv(4))
          raise "Unknown response code #{res_code}. Was expecting (0) for AuthenticationOk" unless res_code == 0
          
          parse_status_params_and_wait_ready_for_query()
        elsif(auth_response == 'E')
          parse_error_response()
        else
          raise "Unknown authentication response code #{auth_rsponse}"
        end
      elsif(auth_code == 'E')
        puts "handling error #{auth_code}"
        parse_error_response()
      else
        raise "Unknown authentication response code #{auth_code}"
      end
      
    end
    
    def startup_message(user, database)
      body = "#{b_int16(3)}#{b_int16(0)}#{c_str('user')}#{c_str(user)}#{c_str('database')}#{c_str(database)}#{c_str('')}"
      length = b_int32(body.length+4)
      "#{length}#{body}"
    end
    
    def execute_query(&block)
      if @query_ready
        @query_ready = false
        yield
      else
        raise "Connection is not ready!! This should not happen!!"  
      end        
    end
    
    def parse_error_response()

      len = int32(@socket.recv(4))
      err = @socket.recv(len)
      arr = err.split(/\x0/)
      Result.new({ :error => Error.new(arr) })
      # need a better way to handle this   
      raise Error.new(arr)
      #raise "Error while connecting to postgres #{err_code} => #{err_msg}"  
    end
    
    def handle_auth(length, code, user, password)
      
      auth_name = @@auth_types.select { |type, values |
        (values[:code] == code && values[:length] == length)
      }
      #puts "auth_name #{auth_name[0][0]}"
      begin
        self.send("do_#{auth_name[0][0].to_s}".to_sym, user, password)
      rescue NoMethodError
        puts "Unsupported authentication #{auth_name}"
        close()
      end
    end
    
    def do_md5(username, password)
      salt = @socket.recv(4)
      @socket.write(md5_auth_message(username, password, salt))            
    end
    
    def do_cleartext(username, password)
      @socket.write(cleartext_auth_message(username, password))                  
    end
    
    def parse_status_params_and_wait_ready_for_query()
      code = @socket.recv(1)
      #puts "Status param code #{code}"
      if(code == 'S')
        process_parameter_status()
      elsif(code == 'K')
        process_backend_key_data()  
      elsif(code == 'Z')
        process_ready_for_query()
      elsif(code == 'N')  
        process_notification_response()
      elsif(code == 'I')
        empty_query_response()
      end
    end
    
    def process_backend_key_data()
      length = int32(@socket.recv(4))
      @pid = int32(@socket.recv(4))
      @secret = int32(@socket.recv(4))
      parse_status_params_and_wait_ready_for_query()
    end
    
    def process_ready_for_query()
      length = int32(@socket.recv(4))
      status  = @socket.recv(1)
      puts "Ready for query status #{status}"
      @query_ready = true   
    end
    
    def parse_row_description()
      length = int32(@socket.recv(4))
      fields = parse_field_descriptions()
      
      next_code = @socket.recv(1)
      #puts "Next code #{next_code}"
      if(next_code == 'D')
        result = { :fields => fields, :rows => [] }
        return parse_data_row(result, 0)
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
        status = command_completed()
        puts "Final row count #{row_count}"
        result[:cmd_status] = status
        result[:cmd_rows] = row_count+1  
        return Result.new(result)
      elsif(next_code == 'D') 
        row_count += 1
        return parse_data_row(result, row_count)
      end    
         
    end
    
    def command_completed()
      length = int32(@socket.recv(4))
      res = @socket.recv(length-4)
      puts "Command completed #{res}"
      parse_status_params_and_wait_ready_for_query()
      res    
    end
    
    def empty_query_response
      #puts "Got empty query"
      consume_empty_response_and_wait_ready_for_query()
    end
    
    def process_notification_response
      len = int32(@socket.recv(4))
      notices = @socket.recv(len-4)
      arr = notices.split(/\x0/)
      puts arr.join(' ')
      parse_status_params_and_wait_ready_for_query
    end
    
    def process_parameter_status
      len = int32(@socket.recv(4))
      status = @socket.recv(len-4)
      
      status.unpack('A*').each { |s|
        pair = s.split(/\x0/)
        val = pair[1].nil? ? '' : pair[1].strip
        #puts "#{pair[0]} => #{val}"
        @parameter_status[pair[0].strip] = val 
      }
       
      parse_status_params_and_wait_ready_for_query()
    end
    
    def portal_suspended_response
      consume_empty_response_and_wait_ready_for_query()
    end
    
    def no_data
      #puts 'No data'
      consume_empty_response_and_wait_ready_for_query()
    end
    
    def consume_empty_response_and_wait_ready_for_query()
      @socket.recv(4)
      parse_status_params_and_wait_ready_for_query()
    end
    
  end
  
  
end