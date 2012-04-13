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

  it "should NOT be able to connect using defaults" do
  
    expect {
      conn = Pg::Connection.new()  
    }.to raise_error(Pg::Error)
    
  end    
  
  it "should be able to connect using default hostname, port, dbname" do
  
    begin
      conn = Pg::Connection.new(nil, nil, nil, nil, nil, @user, @password)
      conn.should_not == nil
      conn.is_a?(Pg::Connection).should == true      
    ensure
      conn.close() unless conn.nil?  
    end
    
  end  
  
  it "should be able to connect using hash" do
      params = { :host => @host, :port => @port, :user => @user, :password => @password }
      verify_simple_connection(params)
  end    
  
  it "should be able to connect using a connection string" do
      params = "host=#{@host} port=#{@port} user=#{@user} password=#{@password}"
      verify_simple_connection(params)
  end      
      
  
  it "should be able to process a simple query" do
  
    begin
      conn = Pg::Connection.new(@host, @port, nil, nil, @dbname, @user, @password)
      result = conn.exec("select NOW() as when")
    ensure
      conn.close() unless conn.nil?  
    end
    
  end  
  
  
  it "should be able to process a multiple rows" do
  
    begin
      conn = Pg::Connection.new(@host, @port, nil, nil, @dbname, @user, @password)
      fields = ["table_schema", "table_name", "table_type"]
      result = conn.exec("SELECT #{fields.join(',')} FROM information_schema.tables order by table_name")
      
      result.fields.should == fields
      table_schema = result.field_values('table_schema')
      table_schema.size.should == 135

      indexed_methods = {:fformat => [0,0], :fname => [0, 'table_schema'], :fsize => [0, 65535], 
                        :ftable => [0, 11500], :ftablecol => [0 ,2]}
        
      indexed_methods.each { | method_name, val  |
        #puts "Invoking #{method_name} with #{val[0]}"
        returned = result.send(method_name, val[0])
        returned.should == val[1]
        
        expect_column_out_of_range {
          result.send(method_name, 3)  
        }        
      }
                
      result.column_values(1).size.should == 135
      expect_column_out_of_range {
        result.column_values(99)
      }
      
      result.fnumber('table_name').should == 1
      result.fnumber('aaa').should == nil
      
      # consider moving this into another IT block as we need to test
      # nil column values
      result.getisnull(0, 0).should == false
      
      expect_argument_error("Invalid row number") {
        result.getisnull(999, 0)  
      }
      
      expect_argument_error("Invalid field position") {
        result.getisnull(0, 4)  
      }

      result.getvalue(0, 0).should == 'information_schema'      
      expect_argument_error("Invalid row number") {
        result.getvalue(999, 0)
      }
      expect_argument_error("Invalid field position") {
        result.getvalue(0, 4) 
      }      
      
      result.getlength(0, 0).should == 18
      
      result.nfields.should == 3
      
      result.ntuples.should == 135
      
      values = result.values
      values.size.should == 135
      values[0].size.should == 3
      
      # This may change with different version of PG
      values = result[0]
      values['table_schema'].should == 'information_schema'
      values['table_name'].should == '_pg_foreign_data_wrappers'
      values['table_type'].should == 'VIEW'
      expect_argument_error('Invalid row number') {
        result[999] 
      }
      
      result.cmd_status.strip!.should == 'SELECT 135'
      result.cmd_tuples.should == 135
      
      result.each { | r |
        r.should_not == nil
      }  
    ensure
      conn.close() unless conn.nil?  
    end
    
  end
  
  it "should be able to handle simple error" do
    conn = standard_connection()
    
    expect {
      result = conn.exec("SELECT * FROM non_existent_table")
    }.to raise_error(Pg::Error)
#    result.error_message.should_not == nil
#    result.error_field(PGresult::PG_DIAG_SEVERITY).should == 'ERROR'
#    result.error_field('*').should == nil
  end
  
  it "should be able to get the status parameters" do
    conn = standard_connection()
    
    params = { 'application_name' => '', 'server_version' => '9.0.4', 'client_encoding' => 'UTF8', 
    'session_authorization' => 'postgres', 'is_superuser' => 'on', 'server_encoding' => 'UTF8' }
    params.each { |k,v|
      p = conn.parameter_status(k)
      p.should == v
    }
  end
  
  def standard_connection
    Pg::Connection.new(@host, @port, nil, nil, @dbname, @user, @password)
  end
  
  def connection_for_testdb
    Pg::Connection.new(@host, @port, nil, nil, 'test', @user, @password)
  end
  
  def expect_argument_error(error_msg, &block)
    expect {
      yield
    }.to raise_error(ArgumentError, error_msg)          
  end
  
  def expect_column_out_of_range(&block)
    expect_argument_error("Column number is out of range", &block)
  end  
  
  def verify_simple_connection(params) 
    begin
      conn = Pg::Connection.new(params)
      conn.should_not == nil
      conn.is_a?(Pg::Connection).should == true
      result = conn.exec("")
      result.should_not == nil
    ensure
      conn.close() unless conn.nil?  
    end

  end
  
end