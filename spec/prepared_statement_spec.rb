require File.dirname(__FILE__) + '/spec_helper'
require 'connection'  

describe "Postgres Connection Prepared Statement" do

  before(:all) do
    @host = 'localhost'
    @port = 5432
    @dbname = 'postgres'
    @user = 'postgres'
    @password = 'postgres'
  end

  it "should be able to prepare a statement" do
    
    begin
      conn = standard_connection()
      name = 'mystmt'
      conn.prepare(name, 'SELECT $1::varchar from information_schema.tables')
    ensure
      conn.close unless conn.nil?
    end  
  end
  
  it "should be able to prepare a statement with param types" do
    
    begin
      conn = standard_connection()
      name = 'mystmt'
      conn.prepare(name, 'SELECT $1 from information_schema.tables', [1043])
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  it "should be able to handle prepared statement syntax error" do
    
    begin
      conn = standard_connection()
      name = 'mystmt'
      expect {
        conn.prepare(name, 'SELECT $1::varchar information_schema.tables')
      }.to raise_error(Pg::Error)
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  it "should be able to handle prepared statement error" do
    
    begin
      conn = standard_connection()
      name = 'mystmt'
      expect {
        conn.prepare(name, 'SELECT $1::varchar from non_existent_table')
      }.to raise_error(Pg::Error)
    ensure
      conn.close unless conn.nil?
    end  
  end    
  
  it "should be able to execute a prepared statement" do
    
    begin
      conn = standard_connection()
      name = 'mystmt'
      conn.prepare(name, "SELECT $1::varchar from information_schema.tables where table_schema='information_schema'")
      result = conn.exec_prepared(name, [ { :value => 'table_schema', :format => 0} ])
      values = result.values()
      values.size.should == 61 #135
      values[0][0].should == 'table_schema'  
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  it "should be able to execute prepared statement with a block" do
    
    begin
      conn = standard_connection()
      name = 'mystmt'
      conn.prepare(name, "SELECT $1::varchar from information_schema.tables where table_schema='information_schema'")
      conn.exec_prepared(name, [ { :value => 'table_schema', :format => 0} ]) { |result|
        values = result.values()
        values.size.should == 61
        values[0][0].should == 'table_schema'  
      }
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  it "should be able to handle error when executing prepared statement with a block" do
    
    begin
      conn = standard_connection()
      name = 'mystmt'
      conn.prepare(name, "SELECT $1::varchar from information_schema.tables where table_schema='information_schema'")
      expect {
        conn.exec_prepared(name, []) { |result|
        }
      }.to raise_error(Pg::Error)
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  it "should be able to to prepare and execute an empty statement" do
    
    begin
      conn = standard_connection()
      name = 'empty'
      conn.prepare(name, '')
      conn.exec_prepared(name)
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  it "should be able to describe an empty prepared statement" do
    
    begin
      conn = standard_connection()
      name = 'empty'
      conn.prepare(name, '')
      result = conn.describe_prepared(name)
      result.is_a?(Pg::Result).should == true
      result.nparams.should == 0
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  it "should be able to describe a prepared statement" do
    
    begin
      conn = standard_connection()
      name = 'mystmt'
      conn.prepare(name, 'SELECT $1::varchar from information_schema.tables')
      result = conn.describe_prepared(name)
      result.is_a?(Pg::Result).should == true
      result.nparams.should == 1
      result.fname(0).should == 'varchar'
    ensure
      conn.close unless conn.nil?
    end  
  end
  
  it "should be able to handle describing non existing prepared statement error" do
    
    begin
      conn = standard_connection()
      name = 'empty'
      conn.prepare(name, '')
      expect {
        conn.describe_prepared('non_existent_stmt')  
      }.to raise_error(Pg::Error)
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  def standard_connection
    Pg::Connection.new(@host, @port, nil, nil, @dbname, @user, @password)
  end  
  
end