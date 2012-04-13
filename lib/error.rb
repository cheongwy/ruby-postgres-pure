module Pg
  
  class Error < Exception
    
    def initialize(err)
      @error = err
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
     
    def message
      error_field(PGresult::PG_DIAG_MESSAGE_PRIMARY)
    end
    
    private
    
  end
  
end