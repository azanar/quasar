require File.expand_path('../test_helper', __FILE__)

require File.expand_path('../db_test_helper', __FILE__)

require 'quasar'

class Quasar::IntegrationTest < Test::Unit::TestCase
  test "stuff" do

    model = Hydrogen::Model.new({:table_name => "foo"})

    source = 100.times.map { ["FOO", "BAR"] }

    Quasar::Loader.load(source, model)
  end
end
