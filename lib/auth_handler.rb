require 'message_generator'

module Pg
  
  module AuthHandler
    include MessageGenerator
    
    AUTH_TYPES = { 
      :kerberos => { :length => 8, :code => 2 },
      :clear => { :length => 8, :code => 3 },
      :md5 => { :length => 12, :code => 5 }
    }    
    
    def do_auth(user, password, dbname)
      
      @socket.write(startup_message(user,dbname))
      res_code = @socket.recv(1)
      
      case res_code
      when 'R'
        length = int32(@socket.recv(4))
        auth_code = int32(@socket.recv(4))
        send_auth_credentials(length, auth_code, user, password)
        process_auth_credentials_response()
      when 'E'
        parse_auth_error_response()
      else
        raise "Unknown authentication response code #{res_code}"
      end
      
    end
    
    private
    def send_auth_credentials(length, code, user, password)
      raise "AuthHandler needs a socket to function correctly" if @socket.nil?

      auth_name = AUTH_TYPES.select { |type, values |
        (values[:code] == code && values[:length] == length)
      }
      #puts "auth_name #{auth_name.keys[0]}"
      begin
        self.send("do_#{auth_name.keys[0].to_s}".to_sym, user, password)
      rescue NoMethodError
        close()
        raise "Unsupported authentication #{auth_name[0][0]}"
      end
    end
    
    def do_md5(username, password)
      salt = @socket.recv(4)
      @socket.write(md5_auth_message(username, password, salt))            
    end
    
    def do_cleartext(username, password)
      @socket.write(cleartext_auth_message(username, password))                  
    end
    
    def startup_message(user, database)
      body = "#{b_int16(3)}#{b_int16(0)}#{c_str('user')}#{c_str(user)}#{c_str('database')}#{c_str(database)}#{c_str('')}"
      length = b_int32(body.length+4)
      "#{length}#{body}"
    end

    def process_auth_credentials_response
      auth_response = @socket.recv(1)
      #puts "Auth response #{auth_response}"
      case auth_response
      when 'R'
        res_len = int32(@socket.recv(4))
        res_code = int32(@socket.recv(4))
        raise "Unknown response code #{res_code}. Was expecting (0) for AuthenticationOk" unless res_code == 0
        
        wait_ready_for_query()
      when 'E'
        parse_auth_error_response()
      else
        raise "Unknown authentication response code #{auth_rsponse}"
      end
    end        
    
    def parse_auth_error_response()

      len = int32(@socket.recv(4))
      err = @socket.recv(len)
      arr = err.split(/\x0/)
      puts "Got auth error #{arr}"
      raise Error.new(arr)
    end   
    
    def wait_ready_for_query()
      code = @socket.recv(1)
      #puts "Status param code #{code}"
      case code
      when 'K'
        process_backend_key_data()  
      when 'S'
        process_parameter_status()
      when 'Z'
        process_ready_for_query()
      when 'E'
        parse_auth_error_response()
      when 'N'  
        process_auth_notification_response()
      else
        raise "Unknown response code #{code}"
      end
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
       
      wait_ready_for_query()
    end
    
    
    def process_backend_key_data()
      length = int32(@socket.recv(4))
      @pid = int32(@socket.recv(4))
      @secret = int32(@socket.recv(4))
      wait_ready_for_query()
    end
    
    def process_auth_notification_response
      len = int32(@socket.recv(4))
      notices = @socket.recv(len-4)
      arr = notices.split(/\x0/)
      puts arr.join(' ')
      wait_ready_for_query()
    end
    
    
  end
  
  
end