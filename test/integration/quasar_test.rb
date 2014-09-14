require File.expand_path('../test_helper', __FILE__)

require File.expand_path('../db_test_helper', __FILE__)

require 'quasar'

class Quasar::IntegrationTest < Test::Unit::TestCase
  test "stuff" do
    @mock_model = mock
    @mock_model.expects(:name).at_least_once.returns("mock_models")

    table_object = Hydrogen::TableObject.new(@mock_model)

    aws_table_object = Hydrogen::AWS::TableObject.new(table_object)

    l = Schlepp::Sink::Fs::Sequencer.new(table_object, :chunk_size => 40000)

  end
end
