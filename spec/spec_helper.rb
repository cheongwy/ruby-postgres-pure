require 'rspec'

$: << "./lib"

def standard_connection
  Pg::Connection.new(@host, @port, nil, nil, @dbname, @user, @password)
end    