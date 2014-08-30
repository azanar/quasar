require File.expand_path('../../../../../test_helper', __FILE__)
require File.expand_path('../db_test_helper', __FILE__)

require 'quasar/aws/redshift/db/table'

class Quasar::AWS::Redshift::DB::TableTest < Test::Unit::TestCase
  include Quasar::AWS::Redshift::DB::DBTestHelper
  setup do
    @connection = mock
    @mock_model = mock
    @table = Quasar::AWS::Redshift::DB::Table.new(@mock_model, @connection)
  end

  test 'loading standard table' do
    @mock_model.expects(:name).at_least_once.returns("mock_table")
    @mock_model.expects(:columns).at_least_once.returns(%w{id mock_col_1 mock_col_2})

    progress = states('progress').starts_as('start')

    @connection.expects(:run_command_with_retry).when(progress.is('start')).then(progress.is('upserted')).with { |args|
      expected = [
        "UPDATE mock_table\n      SET mock_col_1=s.mock_col_1,\nmock_col_2=s.mock_col_2\n        FROM mock_table_staging s\n        WHERE s.id = mock_table.id",
        "INSERT INTO mock_table\n        SELECT s.* FROM mock_table_staging s\n          LEFT JOIN mock_table t\n          ON s.id = t.id\n        WHERE t.id IS NULL" 
      ]
      db_command_order_matches(args, expected)
    }
    @connection.expects(:run_command_with_retry).when(progress.is('upserted')).then(progress.is('done')).with { |args|
      expected = ["VACUUM mock_table",
                    "ANALYZE mock_table"]
      db_command_order_matches(args, expected)
    }

    mock_staging_table = mock
    mock_staging_table.expects(:name).twice.returns("mock_table_staging")

    @table.load(mock_staging_table)
    @table.finalize
  end

  test 'no vacuum when not loaded' do
    @connection.expects(:run_command_with_retry).never
    
    @table.finalize
  end
end
