module Pg
  
  module BinaryConverter
    
    def int16(bint)
      bint.unpack('n*')[0]
    end        
    
    def int32(bint)
      bint.unpack('N*')[0]
    end            
    
    def b_int16(int)
      [int].pack('n')
    end    
    
    def b_int32(int)
      [int].pack('N')
    end
    
    def c_str(str)
      "#{str}\0"
    end
    
  end  
  
end

