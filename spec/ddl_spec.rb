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

  it "should be able to drop a database" do
    
    begin
      conn = standard_connection()
      conn.exec('DROP DATABASE IF EXISTS TEST')
    ensure
      conn.close() unless conn.nil? 
    end  
  end
  
  it "should be able to create a database" do
    
    begin
      conn = standard_connection()
      conn.exec('CREATE DATABASE TEST')
    ensure
      unless conn.nil?
        conn.exec('DROP DATABASE IF EXISTS TEST')
        conn.close() 
      end
    end  
  end  
  
  it "should be able to handle ddl syntax error" do
    
    begin
      conn = standard_connection()
      expect {
        conn.exec('DROP DATABASE IF EXISTS')
      }.to raise_error(Pg::Error)
    ensure
        conn.close() unless conn.nil? 
    end  
  end  
  
  it "should be able to handle ddl error" do
    
    begin
      conn = standard_connection()
      expect {
        conn.exec('DROP DATABASE NON_EXISTENT')
      }.to raise_error(Pg::Error)
    ensure
        conn.close() unless conn.nil? 
    end  
  end    
  
  
  def standard_connection
    Pg::Connection.new(@host, @port, nil, nil, @dbname, @user, @password)
  end    
  
  def connect_with(dbname)
    Pg::Connection.new(@host, @port, nil, nil, dbname, @user, @password)
  end
  
    
end