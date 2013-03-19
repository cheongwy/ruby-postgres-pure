module CharSet
  
  # This map only contains pg encodings that have a different name
  # from Ruby's
  @@map = {
    'UTF8' => 'UTF-8',
    'SQL_ASCII' => 'ASCII-8BIT'  
  }
  
  def self.pg_to_ruby(encoding_name)
    name = @@map[encoding_name] || encoding_name.gsub!('_', '-')
    begin
      Encoding.find(name)
    rescue ArgumentError => e
      nil
    end
  end
  
  def self.ruby_to_pg(encoding)
    encoding_name = encoding.to_s
    @@map[encoding_name] || encoding_name.gsub!('-', '_')
  end  
  
  
  
end