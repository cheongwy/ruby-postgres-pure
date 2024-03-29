require 'binary_converter'
require 'digest/md5'

module Pg
  
  module MessageGenerator
    include BinaryConverter
    
    def md5_auth_message(username, password, salt)
      inner = Digest::MD5.hexdigest("#{password}#{username}")
      outer = Digest::MD5.hexdigest("#{inner}#{salt}")
  
      pass = c_str("md5#{outer}")
      message('p', pass)
    end
    
    def cleartext_auth_message(username, password)
      message('p', c_str(password))
    end
    
    def plain_sql_message(sql)
      message('Q', c_str(sql))
    end
    
    def parse_message(name, query, param_oids=[])
      params_size = param_oids.size
      body = "#{c_str(name)}#{c_str(query)}#{b_int16(params_size)}"
      unless(param_oids.nil?)
        oids = ""
        param_oids.each { |oid|
          oids += b_int32(oid)
        }
        body = body + oids
      end
      message('P', body)
    end
    
    def sync_message
      message('S', '')
    end
    
    def bind_message(name, params)
      fcode_num = b_int16(params.size)
      
      codes = [] 
      values = []
      
      params.each { |p|
        val = p[:value]
        val_len = b_int32(val.bytesize)
        code = b_int16(p[:format] || 0)
        codes << code
        values << "#{val_len}#{val}"    
      }
      
      fp = values.join()
      fcodes = codes.join()
            
      body = "#{c_str('')}#{c_str(name)}#{fcode_num}#{fcodes}#{fcode_num}#{fp}#{fcode_num}#{fcodes}"
      message('B', body)
    end
    
    def describe_portal_message(name='')
      message('D', "P#{c_str(name)}")
    end
    
    def describe_prepared_message(name='')
      message('D', "S#{c_str(name)}")
    end    
    
    def execute_message(limit=0)
      body = "#{c_str('')}#{b_int32(limit)}"
      message('E', body)      
    end
    
    #
    # Close a prepared statement portal
    #
    def close_portal(name='')
      message('C', "P#{c_str(name)}")
    end
    
    #
    # Close a prepared statement
    #
    def close_statement(name='')
      message('C', "S#{c_str(name)}")
    end
    
    def flush_message
      message('H', '')
    end
    
    def cancel_message(pid, secret)
      body = "#{b_int32(80877102)}#{b_int32(pid)}#{b_int32(secret)}"
      message('', body)
    end
        
    def copy_data_message(data='')
      # data does not need to be convereted by c_str
      message('d', data)
    end
    
    def copy_done_message()
      message('c', '')
    end
    
    def copy_fail_message(cause='')
      message('f', c_str(cause))
    end    
    
    private
    
    def message(code, body)
      length = b_int32(body.length+4)
      msg = "#{code}#{length}#{body}"
    end
    
  end
  
end