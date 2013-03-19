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
  
  it "should be able to return basic params" do
    
    begin
      conn = Pg::Connection.new(@host, @port, {:test=>true}, nil, @dbname, @user, @password)
      conn.options[:test].should == true
      conn.host.should == @host
      conn.port.should == @port
      conn.user.should == @user
      conn.pass.should == @user  
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  it "should be able to reset the connection" do
    
    begin
      conn = standard_connection()
      conn.reset()
      conn.closed?.should == false
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  it "should be able to get the server version" do
    
    begin
      conn = standard_connection()
      conn.server_version.should_not == nil
      conn.server_version.should_not == ''
    ensure
      conn.close unless conn.nil?
    end  
  end
  
  it "should be able to get and set the client encoding" do
    
    begin
      conn = standard_connection()
      conn.set_client_encoding('SQL_ASCII')
      conn.get_client_encoding.should.to_s == 'SQL_ASCII' 
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  it "should be able to set the default encoding" do
    
    begin
      conn = standard_connection()
      def_enc = conn.set_default_encoding
      conn.get_client_encoding.should.to_s == nil
      def_enc.should == nil
      
      # Set Ruby's default encoding and then test again
      Encoding.default_internal = Encoding::UTF_8
      def_enc = conn.set_default_encoding
      def_enc.should_not == nil
      def_enc.to_s == 'UTF-8'
      conn.get_client_encoding.should.to_s == 'UTF-8'
    ensure
      conn.close unless conn.nil?
    end  
  end  
  
  def standard_connection
    Pg::Connection.new(@host, @port, nil, nil, @dbname, @user, @password)
  end      
  
  
end