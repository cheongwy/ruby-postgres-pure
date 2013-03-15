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

  it "should be able to get backend pid" do
  
    begin
      conn = standard_connection
      
      conn.backend_pid.should_not == nil      
    ensure
      conn.close() unless conn.nil?  
    end
    
  end
  
  it "should be able to issue cancel request" do
  
    begin
      conn = standard_connection
      
      conn.exec('select NOW() as when')
      conn.cancel()
      
      conn.exec('select NOW() as when')      
    ensure
      conn.close() unless conn.nil? 
    end
    
  end
  
  it "should be able to provide error message" do
    
    begin
      conn = standard_connection
      expect {
        conn.exec("select")  
      }.to raise_error(Pg::Error)
      conn.error_message().should_not == nil  
      conn.error_message().should == 'syntax error at end of input'
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  it "should be able to return the server encoding" do
    
    begin
      conn = standard_connection()
      conn.external_encoding.should_not == nil
      conn.external_encoding.should_not == ''
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  it "should be able to return the client encoding" do
    
    begin
      conn = standard_connection()
      conn.get_client_encoding.should_not == nil
      conn.get_client_encoding.should_not == ''
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  def standard_connection
    Pg::Connection.new(@host, @port, nil, nil, @dbname, @user, @password)
  end      
  
  
end