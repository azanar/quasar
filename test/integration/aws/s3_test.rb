require File.expand_path('../../test_helper', __FILE__)

require File.expand_path('../../db_test_helper', __FILE__)

require 'quasar/aws/s3'
require 'quasar/table_object'

class Quasar::AWS::S3::IntegrationTest < Test::Unit::TestCase
  setup do
    @stub_s3_bucket = stub
    Quasar::AWS::S3.stubs(:bucket).returns(@stub_s3_bucket)

    @stub_s3_objects = stub
    @stub_s3_bucket.stubs(:objects).returns(@stub_s3_objects)

    @mock_s3_object = mock
    @stub_s3_objects.stubs(:[]).returns(@mock_s3_object)

    @mock_model = mock
    @mock_model.expects(:name).at_least_once.returns("mock_models")
  end

  test 'stuff' do
    table_object = Quasar::TableObject.new(@mock_model)

    s3_table_object = Quasar::AWS::S3::TableObject.new(table_object)

    loader = Quasar::Sink::Loader.new(s3_table_object)

    @mock_s3_object.expects(:write)
    loader.write(["FOO|BAR|BAZ"] * 10)

    loader.finalize
  end
end

