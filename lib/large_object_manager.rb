module Pg
  
  module LargeObjectManager
    
    def lo_close(lo_desc)
      exec("select lo_close(#{lo_desc});")
    end
    alias_method :loclose, :lo_close
    
    def lo_creat(mode=-1)
      res = exec('select lo_creat(-1);')
      oid = res.getvalue(0,0)
      oid.to_i   
    end
    alias_method :locreat, :lo_creat
    
    def lo_create(oid)
      res = exec("select lo_create(#{oid});")
      oid = res.getvalue(0,0)
      oid.to_i   
    end    
    alias_method :locreate, :lo_create 
    
    def lo_export(oid, filepath)
      begin
        exec("BEGIN;")
        lod = lo_open(oid, Pg::INV_READ)
        
        data = ''
        while (chunk = lo_read(lod, 8096)) != (nil || '')
          data << chunk
        end
        exec("COMMIT;")
        puts "Exported #{data}"
        File.open(filepath, "wb") {|io| io.write(data) }
        nil
      rescue Exception => e
        #puts "Encountered exception while importing large object #{e.message}"
        raise Error.new(e)
      end      
    end
    alias_method :loexport, :lo_export
    
    def lo_import(filepath)
      
      begin
        exec("BEGIN")
        oid = lo_creat() 
        lod = lo_open(oid, Pg::INV_WRITE)
        
        # consider buffering content
        contents = File.open(filepath, "rb") {|io| io.read }
        lo_write(lod, contents)
        lo_close(lod)
        exec("COMMIT")      
        return oid.to_i
      rescue Exception => e
        #puts "Encountered exception while importing large object #{e.message}"
        raise Error.new(e)
      end
      
    end
    alias_method :loimport, :lo_import
    
    def lo_lseek(lo_desc, offset, whence)
      res = exec("select lo_lseek(#{lo_desc}, #{offset}, #{whence});")
      pos = res.getvalue(0,0)
      pos.to_i
    end
    alias_method :lo_seek, :lo_lseek
        
    def lo_open(oid, mode=Pg::INV_READ)
      res = exec("select lo_open(#{oid}, #{mode});")
      lod = res.getvalue(0,0)      
    end
    alias_method :loopen, :lo_open
        
    def lo_read(lo_desc, len)
      res = exec("select loread(#{lo_desc}, #{len});")
      val = res.getvalue(0,0)
      unless val.nil? || val == ''
        val = [val].pack('H*')
        val = val.slice(1, val.length)
      end
      #puts "Read #{val}"
      val
    end
    alias_method :loread, :lo_read
    
    def lo_tell(lo_desc)
      res = exec("select lo_tell(#{lo_desc});")
      pos = res.getvalue(0,0)
      pos.to_i      
    end
    alias_method :lotell, :lo_tell
    
    def lo_truncate(lo_desc, len)
      exec("select lo_truncate(#{lo_desc}, #{len});")
      nil
    end
    alias_method :lotruncate, :lo_truncate
    
    def lo_unlink(oid)
      exec("select lo_unlink(#{oid});")
    end
    alias_method :lounlink, :lo_unlink
    
    def lo_write(lo_desc, buffer)
      name = 'lo_stmt'
      prepare(name, "select lowrite(#{lo_desc}, $1);")
      result = exec_prepared(name, [ { :value => buffer, :format => 1} ])
      written = buffer.bytesize
#      written = result.values.join()
      puts "Written #{written}"
      written   
    end
    alias_method :lowrite, :lo_write     
    
  end
  
  
end