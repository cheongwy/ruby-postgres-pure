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
  
  it "should be able to execute copy out to STDOUT queries and provide the results" do
    
    begin
      conn = standard_connection()
      res = conn.query("COPY (select table_name, table_type, table_schema FROM information_schema.tables where table_schema='information_schema') to STDOUT;")
      res.result_status.should == Pg::Result::PGRES_COPY_OUT
      
      count = 0
      while (data = conn.get_copy_data) != nil
        data.should_not == nil
        count = count + 1
      end
      data.should == nil
      count.should == 61      
            
      # make sure we can still make subsequent queries
      res = conn.query('')
      res.result_status.should == Pg::Result::PGRES_EMPTY_QUERY
      
    ensure
      conn.close unless conn.nil? or conn.closed?
    end  
  end
  
  it "should be able to execute copy in command" do
    
    begin
      conn = standard_connection()
      conn.query('drop table if exists test_table;')
      conn.query('create table test_table (code varchar(3), name varchar(40));')
      res = conn.query("copy test_table from STDIN (DELIMITER '|');")
      sent = conn.put_copy_data('AF | AFGHANISTAN')
      sent.should == true
      ended = conn.put_copy_end()
      ended.should == true
      res.result_status.should == Pg::Result::PGRES_COPY_IN
      # make sure we can still make subsequent queries
      res = conn.query('')
      res.result_status.should == Pg::Result::PGRES_EMPTY_QUERY
    ensure
      conn.query('drop table if exists test_table;') unless conn.closed?
      conn.close unless conn.nil? or conn.closed?
    end  
  end
  
  it "should be able to execute copy out to file" do
    
    begin
      conn = standard_connection()
      filepath = '/tmp/copy_data'
      res = conn.query("COPY (select 0 from pg_tables limit 0) to '#{filepath}';")
      File.exists?(filepath).should == true
      File.size(filepath).should == 0
      res.result_status.should == Pg::Result::PGRES_COMMAND_OK
      
      res = conn.query("COPY (select table_name, table_type, table_schema FROM information_schema.tables where table_schema='information_schema') to '#{filepath}';")
      res.result_status.should == Pg::Result::PGRES_COMMAND_OK
      File.size(filepath).should > 0
      #
    ensure
      conn.close unless conn.nil? or conn.closed?
    end  
  end
  
  it "should be able to execute copy in command and terminate it if another query follows" do
    
    begin
      conn = standard_connection()
      conn.query('drop table if exists test_table;')
      conn.query('create table test_table (code varchar(3), name varchar(40));')
      res = conn.query("copy test_table from STDIN (DELIMITER '|');")
      res.result_status.should == Pg::Result::PGRES_COPY_IN
      # make sure we can still make subsequent queries
      res = conn.query('')
      res.result_status.should == Pg::Result::PGRES_EMPTY_QUERY
    ensure
      conn.query('drop table if exists test_table;') unless conn.closed?
      conn.close unless conn.nil? or conn.closed?
    end  
  end
  
  it "should be able to execute copy in command and terminate it if another execution follows" do
    
    begin
      conn = standard_connection()
      conn.query('drop table if exists test_table;')
      conn.query('create table test_table (code varchar(3), name varchar(40));')
      res = conn.query("copy test_table from STDIN (DELIMITER '|');")
      res.result_status.should == Pg::Result::PGRES_COPY_IN
      # make sure we can still make subsequent queries
      res = conn.prepare('mystmt', 'SELECT $1::varchar from information_schema.tables')
      res.is_a?(Pg::Result).should == true
      res.nparams.should == 0
      res.fields.size.should == 0
    ensure
      conn.query('drop table if exists test_table;') unless conn.closed?
      conn.close unless conn.nil? or conn.closed?
    end  
  end
  
  
end