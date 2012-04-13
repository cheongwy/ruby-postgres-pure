require File.dirname(__FILE__) + '/spec_helper'
require 'connection'  

describe "Postgres Connection Prepared Statement" do

  before(:all) do
    @host = 'localhost'
    @port = 1234
    @dbname = 'postgres'
    @user = 'postgres'
    @password = 'postgres'
  end

  it "should be able to prepare a statement" do
    
    begin
      conn = standard_connection()
      name = 'mystmt'
      conn.prepare(name, 'SELECT $1::varchar from information_schema.tables')
      #conn.exec_prepared(name, [ { :value => 'table_schema', :format => 0} ])
      #puts "Done"
    ensure
      conn.close unless conn.nil?
    end  
  end
  
  it "should be able to execute a prepared statement" do
    
    begin
      conn = standard_connection()
      name = 'mystmt'
      conn.prepare(name, 'SELECT $1::varchar from information_schema.tables')
      result = conn.exec_prepared(name, [ { :value => 'table_schema', :format => 0} ])
      table_schema = result.field_values('table_schema')
      table_schema.size.should == 135  
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
      puts "Done"
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  
  def standard_connection
    Pg::Connection.new(@host, @port, nil, nil, @dbname, @user, @password)
  end  
  
end