require File.dirname(__FILE__) + '/spec_helper'
require 'connection'  

describe "Postgres Connection" do

  before(:all) do
    @host = 'localhost'
    @port = 5432
    @dbname = 'postgres'
    @user = 'postgres'
    @password = 'postgres'
  end

  it "should be able to import large object from file" do
    
    begin
      conn = standard_connection()
      oid = conn.lo_import('./data/ruby-logo-B.png')
      oid.should_not == nil
      oid.should > 0
      res = conn.exec("select loid from pg_largeobject where loid=#{oid}")
      loid = res.getvalue(0,0)
      oid.should == loid.to_i
    ensure
      conn.lo_unlink(oid) unless oid.nil?
      conn.close unless conn.nil? or conn.closed?
    end  
  end
  
  it "should throw error if file cannot be found for importing" do
    
    begin
      conn = standard_connection()
      expect {
        conn.lo_import('no_existent_file')
      }.to raise_error(Pg::Error)
    ensure
      conn.close unless conn.nil? or conn.closed?
    end  
  end
  
  it "should be able to create a large object and return a oid" do
    
    begin
      conn = standard_connection
      oid = conn.lo_creat()
      
      oid.should_not == nil
      oid.should > 0
    ensure
      conn.lo_unlink(oid) unless oid.nil?
      conn.close unless conn.nil?
    end  
  end
  
  it "should be able to create a large object with the specified oid" do
    test_oid = 12345
    begin
      conn = standard_connection
      oid = conn.lo_create(test_oid)
      oid.should == test_oid
    ensure
      conn.lo_unlink(oid) unless oid.nil?
      conn.close unless conn.nil?
    end  
  end
  
  it "should be raise Pg::Error if creating new large object with an existing oid" do
    
    begin
      conn = standard_connection
      oid = conn.lo_creat()
      
      oid.should_not == nil
      oid.should > 0
      
      expect {
        conn.lo_create(oid)
      }.to raise_error(Pg::Error)
    ensure
      conn.lo_unlink(oid) unless oid.nil?
      conn.close unless conn.nil?
    end  
  end  
  
  it "should be able to handle lo_close error" do
    
    begin
      conn = standard_connection
      expect {
        conn.lo_close(123)  
      }.to raise_error(Pg::Error)
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  it "should be open a large object for reading and close it" do
    
    begin
      conn = standard_connection
      oid = conn.lo_creat()
      
      oid.should_not == nil
      oid.should > 0
      conn.exec("BEGIN")
      lod = conn.lo_open(oid)
      lod.should_not == nil
      
      conn.lo_close(lod)
      conn.exec("COMMIT")
    ensure
      conn.lo_unlink(oid) unless oid.nil?
      conn.close unless conn.nil?
    end  
  end
  
  it "should be able to open a large object for writing and read back the same data" do
    
    begin
      conn = standard_connection
      oid = conn.lo_creat()
      
      oid.should_not == nil
      oid.should > 0
      conn.exec("BEGIN")
      lod = conn.lo_open(oid, Pg::INV_WRITE)
      lod.should_not == nil
      
      content = 'ABC'
      bytes_written = conn.lo_write(lod, content)
      conn.lo_close(lod)
      
      lod = conn.lo_open(oid, Pg::INV_READ)
      read = conn.lo_read(lod, 382)
      #puts "Read #{read}"
      read.should == content
      conn.lo_close(lod)
      conn.exec("COMMIT")
    ensure
      conn.lo_unlink(oid) unless oid.nil?
      conn.close unless conn.nil?
    end  
  end
  
  it "should be able to export a large object" do

    filename = 'data/export'
    
    begin
      conn = standard_connection
      oid = conn.lo_creat()
      
      oid.should_not == nil
      oid.should > 0
      conn.exec("BEGIN")
      lod = conn.lo_open(oid, Pg::INV_WRITE)
      lod.should_not == nil
      
      content = 'ABC'
      bytes_written = conn.lo_write(lod, content)
      conn.lo_close(lod)
      conn.exec("COMMIT")
      
      res = conn.lo_export(oid, filename)
      res.should == nil
      File.size(filename).should == 3
      
    ensure
      File.delete(filename) if File.exist?(filename)
      conn.lo_unlink(oid) unless oid.nil?
      conn.close unless conn.nil?
    end  
  end
  
  it "should be able to seek and tell the position of a large object" do

    filename = 'data/export'
    
    begin
      conn = standard_connection
      oid = conn.lo_import('./data/test.txt')
      
      conn.exec('BEGIN;')
      lod = conn.lo_open(oid, Pg::INV_READ)
      
      # SEEK_SET to set current pos      
      loc = conn.lo_lseek(lod, 3, Pg::SEEK_SET)
      loc.should == 3
      read = conn.lo_read(lod, 3)
      read.should == 'def'

      # seek from current pos
      loc = conn.lo_lseek(lod, 3, Pg::SEEK_CUR)
      read = conn.lo_read(lod, 3)
      read.should == 'jkl'
      
      
      loc = conn.lo_tell(lod)
      # loc should now be at 12 after 2 seeks and 2 reads
      loc.should == 12
      
      # get seek end position with offset      
      loc = conn.lo_lseek(lod, 5, Pg::SEEK_END)
      loc.should == 21 # 16 (length of content) + 5 (offset)
      
      conn.lo_close(lod);      
      conn.exec('COMMIT;')
    ensure
      conn.lo_unlink(oid) unless oid.nil?
      conn.close unless conn.nil?
    end  
  end
  
  it "should be able to truncate a large object" do

    filename = 'data/export'
    
    begin
      conn = standard_connection
      oid = conn.lo_import('./data/test.txt')
      
      conn.exec('BEGIN;')
      lod = conn.lo_open(oid, Pg::INV_WRITE)
      
      conn.lo_truncate(lod, 3)
      read = conn.lo_read(lod, 5)
      # should only be left with 1st 3 chars
      read.should == 'abc'
      
      conn.lo_close(lod);      
      conn.exec('COMMIT;')
    ensure
      conn.lo_unlink(oid) unless oid.nil?
      conn.close unless conn.nil?
    end  
  end  
  
  
end