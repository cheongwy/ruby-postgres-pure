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
      
  it "should fail to connect with non existent DB" do
    expect {
      Pg::Connection.new(@host, @port, nil, nil, 'no_such_db', @user, @password)
    }.to raise_error(Pg::Error)
  end      
  
  
  it "should be able to get the status parameters" do
    conn = standard_connection()
    
    params = { 'application_name' => '', 'server_version' => '9.1.4', 'client_encoding' => 'UTF8', 
    'session_authorization' => 'postgres', 'is_superuser' => 'on', 'server_encoding' => 'UTF8' }
    params.each { |k,v|
      p = conn.parameter_status(k)
      p.should == v
    }
  end
  
  it "should be able to return the correct finished status" do
    
    begin
      conn = standard_connection()
      conn.finished?.should == false
      conn.close()
      conn.finished?.should == true
    ensure
      conn.close unless conn.nil? || conn.closed?
    end  
  end  
  
  def standard_connection
    Pg::Connection.new(@host, @port, nil, nil, @dbname, @user, @password)
  end
  
  def connection_for_testdb
    Pg::Connection.new(@host, @port, nil, nil, 'test', @user, @password)
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