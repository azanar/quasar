require File.expand_path('../../../test_helper', __FILE__)

require 'quasar/sink/loader'
require 'quasar/sink/loader/data_object'

class Quasar::Sink::LoaderTest < Test::Unit::TestCase
  include TestHelper 
  setup do
    @mock_table_object = mock

    @mock_data_object = mock
    Quasar::Sink::Loader::DataObject.expects(:new).with(@mock_table_object).returns(@mock_data_object)

    @s3_loader = Quasar::Sink::Loader.new(@mock_table_object)
  end

  test '#write' do
    @mock_data_object.expects(:write).with("FOO|BAR|BAZ")
    @s3_loader.write(["FOO|BAR|BAZ"])
  end

  test '#finalize' do
    @mock_data_object.expects(:finalize)

    @s3_loader.finalize
  end

end

