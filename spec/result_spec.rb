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

  
  it "should be able to truncate a large object" do

    filename = 'data/export'
    
    begin
      conn = standard_connection
      #conn
    ensure
    end  
  end
  
  
end