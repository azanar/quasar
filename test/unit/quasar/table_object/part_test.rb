require File.expand_path('../../../test_helper', __FILE__)

require 'quasar/table_object/part'

class Quasar::TableObject::Part::Test < Test::Unit::TestCase
  include TestHelper 
  setup do
    @mock_table_object = mock
    @mock_table_object_name = "mock_table_name"

    @table_object = Quasar::TableObject::Part.new(1, @mock_table_object)
  end

  test "#path" do
    @mock_table_object.expects(:name).twice.returns(@mock_table_object_name)
    res = @table_object.path

    assert_equal res, 'mock_table_name/mock_table_name.1.psv.gz'
  end

  test "#path should cache" do
    @mock_table_object.expects(:name).twice.returns(@mock_table_object_name)
    anterior = @table_object.path

    posterior = @table_object.path

    assert_equal anterior, posterior
  end
end


