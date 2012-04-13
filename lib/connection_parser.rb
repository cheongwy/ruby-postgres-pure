module Pg
  
  module ConnectionParser
    
    def parse_as_string(conn)
      args = conn.split(' ')
      params = args.inject({}) { |result, p|
        pair = p.split('=')
        name = pair[0].to_sym
        result[name] = pair[1]
        result  
      }
      parse_as_hash(params)
    end
    
    def parse_as_hash(conn)
      mapping = { :host => 0, :port => 1, :options => 2, :tty => 3, :dbname => 4, :user => 5, :password => 6 }  

      params = []
      mapping.each { | k, v |
        params[v] = conn[k]
      }
      params
      
    end
    
  end
  
end