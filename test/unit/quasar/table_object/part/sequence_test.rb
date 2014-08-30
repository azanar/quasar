require File.expand_path('../../../../test_helper', __FILE__)

require 'quasar/table_object/part/sequence'

class Quasar::TableObject::Part::Sequence::Test < Test::Unit::TestCase
  include TestHelper 
  setup do
    @mock_table_object = mock
    @mock_table_object_name = "mock_table_name"

    @mock_parts = 5.times.map { mock }

    @sequence = Quasar::TableObject::Part::Sequence.new(@mock_table_object, @mock_parts)
  end

  test "#parts" do
    #@mock_table_object.expects(:name).twice.returns(@mock_table_object_name)

    assert_equal @sequence.parts, @mock_parts
  end
end

