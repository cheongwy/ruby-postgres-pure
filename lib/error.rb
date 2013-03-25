module Pg
  
  class Error < Exception

    attr_reader :result, :connection, :message
        
    def initialize(*err)
      
      if err[0].is_a?(Array)
        @error = err[0] 
        @message = error_field(PGresult::PG_DIAG_MESSAGE_PRIMARY)
      elsif err[0].is_a?(Exception)
        @exception = err[0]
        @message = @exception.message()
      elsif err[0].is_a?(String)
        @message = err[0]
      end

      # set the connection and result object if available      
      @connection = err[1] if err.size == 2
      @result = err[2] if err.size == 3
    end
    
    def error_message
      @error.collect { |e|
        e.slice(1, e.length)
      }.join(' ')
    end
  
    def error_field(fieldcode)
      fields = @error.select { |e|
        e.index(fieldcode) == 0
      }
      unless fields.nil? or fields.size == 0
        f = fields[0]
        f.slice(1, f.length)
      else
        nil
      end
    end
     
    
  end
  
end