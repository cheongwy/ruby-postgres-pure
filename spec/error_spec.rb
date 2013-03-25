require File.dirname(__FILE__) + '/spec_helper'
require 'connection'  

describe "Postgres Error" do

  it "should be able to create a new error based on server returned error" do
    
    arr = ["SFATAL", "C3D000", "Mdatabase 'no_such_db' does not exist", "Fpostinit.c", "L709", "RInitPostgres"]    
    err = Pg::Error.new(arr)
    err.message.should == "database 'no_such_db' does not exist"
  end
  
  it "should be able to create a new error given an exception" do
    
    err = Pg::Error.new(Exception.new('Some error'))
    err.message.should == "Some error"
  end
  
  it "should be able to create a new error given a string" do
    
    err = Pg::Error.new('Some error')
    err.message.should == "Some error"
  end
    
  
end