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

  
  it "should be able to process a simple query" do
  
    begin
      conn = Pg::Connection.new(@host, @port, nil, nil, @dbname, @user, @password)
      result = conn.exec("select NOW() as when")
    ensure
      conn.close() unless conn.nil?  
    end
    
  end  
  
  
  it "should be able to process a multiple rows" do
  
    expected_rows = 61
    begin
      conn = Pg::Connection.new(@host, @port, nil, nil, @dbname, @user, @password)
      fields = ["table_schema", "table_name", "table_type"]
      result = conn.exec("SELECT #{fields.join(',')} FROM information_schema.tables where table_schema='information_schema' order by table_name")
      
      result.fields.should == fields
      table_schema = result.field_values('table_schema')
      table_schema.size.should == expected_rows

      # Test the various methods of PG:::Result
      indexed_methods = {:fformat => [0,0], :fname => [0, 'table_schema'], :ftablecol => [0 ,2]}
        
      indexed_methods.each { | method_name, val  |
        #puts "Invoking #{method_name} with #{val[0]}"
        returned = result.send(method_name, val[0])
        returned.should == val[1]
        
        expect_column_out_of_range {
          result.send(method_name, 3)  
        }        
      }
      
      # Test the ftable and fsize method seperately since we won't know 
      # what is the oid of the 1st table or the size of the datatype
      result.ftable(0).should > 0
      result.fsize(0).should > 0
                
      result.column_values(1).size.should == expected_rows
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
      
      result.ntuples.should == expected_rows
      
      values = result.values
      values.size.should == expected_rows
      values[0].size.should == 3
      
      # This may change with different version of PG
      values = result[0]
      values['table_schema'].should == 'information_schema'
      values['table_name'].should == '_pg_foreign_data_wrappers'
      values['table_type'].should == 'VIEW'
      expect_argument_error('Invalid row number') {
        result[999] 
      }
      
      result.cmd_status.strip!.should == "SELECT #{expected_rows}"
      result.cmd_tuples.should == expected_rows
      
      result.each { | r |
        r.should_not == nil
      }  
    ensure
      conn.close() unless conn.nil?  
    end
    
  end
  
  it "should be able to handle simple error" do
    begin
      conn = standard_connection()
      
      expect {
        result = conn.exec("SELECT * FROM non_existent_table")
      }.to raise_error(Pg::Error)
      
      # make sure connection is not closed and we can make another query
      conn.exec("select NOW() as when")
    ensure
      conn.close() unless conn.nil?
    end
#    result.error_message.should_not == nil
#    result.error_field(PGresult::PG_DIAG_SEVERITY).should == 'ERROR'
#    result.error_field('*').should == nil
  end
  
  it "should be able to execute query with a block" do
    
    begin
      conn = standard_connection
      block_result = conn.exec("select 1 as num") { |result|
        result.should_not == nil
        result.nfields.should == 1
        # The returned type of String is correct
        # Checked with original pg lib
        result.getvalue(0,0).should == "1"
        result.getvalue(0,0)
      }
      
      block_result.should == "1"
    ensure
      conn.close unless conn.nil?
    end  
  end
  
  it "should be able to handle errors when executing query with a block" do
    
    begin
      conn = standard_connection
      expect {
        conn.exec("select") { |result|
        }
      }.to raise_error(Pg::Error)
      
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  def standard_connection
    Pg::Connection.new(@host, @port, nil, nil, @dbname, @user, @password)
  end
  
  def expect_argument_error(error_msg, &block)
    expect {
      yield
    }.to raise_error(ArgumentError, error_msg)          
  end
  
  def expect_column_out_of_range(&block)
    expect_argument_error("Column number is out of range", &block)
  end  
  
  
end