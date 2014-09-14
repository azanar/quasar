require File.expand_path('../../test_helper', __FILE__)

require 'hydrogen/table_object'

class Quasar::TableObjectTest < Test::Unit::TestCase
  include TestHelper 
  setup do
    @mock_table_object = mock
    @mock_table_object_name = "mock_table_name"

    @mock_table_object.expects(:name).returns(@mock_table_object_name)

    @table_object = Quasar::TableObject.new(@mock_table_object)
  end
end
