require "rubygems"
require "rspec/core/rake_task"

spec_opts = %w{--colour --format progress}

begin
  desc "Run those specs"
  task :spec do
    RSpec::Core::RakeTask.new(:spec) do |t|
      t.rspec_opts = spec_opts
      t.pattern = 'spec/prepared_statement_spec.rb'
    end
  end
rescue LoadError
    task :spec do
      abort "Rspec is not available. In order to run rspec, you must: (sudo) gem install rspec"
    end
end

