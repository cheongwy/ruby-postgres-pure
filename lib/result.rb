module Pg
  
  class Result
    
    PGRES_EMPTY_QUERY = 0
    PGRES_COMMAND_OK = 1
    PGRES_TUPLES_OK = 2
    PGRES_COPY_OUT = 3
    PGRES_COPY_IN = 4
    PGRES_BAD_RESPONSE =5
    PGRES_NONFATAL_ERROR = 6
    PGRES_FATAL_ERROR = 7
    PGRES_COPY_BOTH = 8
    
    def initialize(result)

#     @result is an internal map that holds the result
#     of the query execution with a structure like below
#                       
#      {   :fields => [
#              { field_name => attributes }
#          ],
#          
#          :rows => [
#              { values => [] }
#          ],
#          :cmd_status => '',
#          :cmd_rows => n,   
#          :error => Error
#          :result_status => n
#      }
      @result = result
    end

    def [](n)
      rows = @result[:rows]
      
      raise ArgumentError.new("Invalid row number") if n > rows.size-1  

      fields = @result[:fields]
      row_val = rows[n][:values]
      hash = {}
      fields.each_index { |i|
        f = fields[i]
        name = f.keys[0]
        hash[name] = row_val[i]
      }
      hash
    end   
    
    def cmd_status
      @result[:cmd_status]
    end 
    
    def cmd_tuples
      @result[:cmd_rows]
    end
    
    def column_values(n)
      ensure_column_range(n)
      
      @result[:rows].collect { | row |
        row[:values][n]
      }      
    end
    
    def each
      fields = @result[:fields]
      cols = fields.size - 1
      
      @result[:rows].each { | row |
        #puts "#{row[:values]}"
        hash = {}
        (0..cols).each { |i|
          name = fields[i].keys[0]
          hash[name] = row[:values][i]
        }
        yield hash
      }
    end
    
    def error_field(fieldcode)
      @result[:error].error_field(fieldcode)
    end
    
    def error_message
      @result[:error].error_message
    end
    
    def fields
      @result[:fields].collect { | field |
        field.keys[0]
      }
    end
    
    def field_values(field)
      index = fnumber(field)
      puts "Rows #{@result[:rows].size}"
      @result[:rows].collect { | row |
        row[:values][index]
      }
    end

    def fformat(column_number)
      field_attribute(column_number, :format_code)
    end
    
    def fmod(column_number)
      field_attribute(column_number, :type_modifier)
    end
    
    def fname(column_number)
      ensure_column_range(column_number)

      column = @result[:fields][column_number]
      return column.keys[0]      
    end
    
    def fnumber(name)
      index = @result[:fields].index { | f |
        f.keys[0] == name
      }            
    end
    
    def fsize(column_number)
      field_attribute(column_number, :data_type_size)
    end
    
    def ftype(column_number)
      field_attribute(column_number, :type_modifier)
    end
    
    def ftable(column_number)
      field_attribute(column_number, :table_id)
    end
    
    def ftablecol(column_number)      
      field_attribute(column_number, :attr_num)
    end
    
    def getisnull(row_position, field_position)
      rows = @result[:rows]
      raise ArgumentError.new("Invalid row number") if rows.size-1 < row_position 
      
      values = rows[row_position][:values]
      raise ArgumentError.new("Invalid field position") if values.size-1 < field_position
        
      value = values[field_position]
      value.nil? ? true : false
    end

    def getlength(row_position, field_position)
      getvalue(row_position, field_position).length
    end
    
    def getvalue(row_position, field_position)
      rows = @result[:rows]
      raise ArgumentError.new("Invalid row number") if rows.size-1 < row_position 
      
      values = rows[row_position][:values]
      raise ArgumentError.new("Invalid field position") if values.size-1 < field_position      

      values[field_position]      
    end
       
    def nfields
      @result[:fields].size
    end
     
    def nparams
      @result[:fields].size
      #raise NotImplementedError("Not implemented yet")
    end
    
    def ntuples
      @result[:rows].size
    end

    def res_status(status)
      raise NotImplementedError("Not implemented yet")
    end
    
    def result_status
      @result[:result_status]
      #raise NotImplementedError("Not implemented yet")
    end
    
    def values
      rows = @result[:rows]
      rows.collect { | row |
        row[:values]
      }  
    end
        
    #
    # Clears the result of the query.
    #
    def clear
      @result = nil
    end
    
    
    private
    
    def ensure_column_range(n)
      raise ArgumentError.new("Column number is out of range") unless @result[:fields].size > n
    end
    
    def field_attribute(column_number, sym_name)
      ensure_column_range(column_number)
      column = @result[:fields][column_number]
      return column.values[0][sym_name]                      
    end
    
  end
  
end

module PGresult
  
  PG_DIAG_SEVERITY = 'S'

  PG_DIAG_SQLSTATE = 'C'

  PG_DIAG_MESSAGE_PRIMARY = 'M'

  PG_DIAG_MESSAGE_DETAIL = 'D'

  PG_DIAG_MESSAGE_HINT = 'H'

  PG_DIAG_STATEMENT_POSITION = 'P'

  PG_DIAG_INTERNAL_POSITION = 'p'

  PG_DIAG_INTERNAL_QUERY = 'q'

  PG_DIAG_CONTEXT = 'W'

  PG_DIAG_SOURCE_FILE = 'F'

  PG_DIAG_SOURCE_LINE = 'L'

  PG_DIAG_SOURCE_FUNCTION = 'R'

end