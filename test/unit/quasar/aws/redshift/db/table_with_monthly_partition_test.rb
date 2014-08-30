require File.expand_path('../../../../../test_helper', __FILE__)
require File.expand_path('../db_test_helper', __FILE__)

class Quasar::AWS::Redshift::DB::TableWithMonthlyPartitionTest < Test::Unit::TestCase
  include Quasar::AWS::Redshift::DB::DBTestHelper

  setup do
    @test_host = "redshift-test.sjc.carrel.org"
    @test_table = "mock_table"
    @test_staging_table = "#{@test_table}_staging"

    @connection = mock

    @mock_model = mock
    @mock_model.expects(:name).at_least_once.returns("mock_table")

  end

  test 'creating monthly partitions over a range' do
    start_time = 2.months.ago
    end_time = Time.now

    tables = Quasar::AWS::Redshift::DB::TableWithMonthlyPartition.from_date_range(@mock_model, @connection, {:min => start_time, :max => end_time})

    assert_equal 3, tables.length
  end

  test 'loading date ranged table needing a new table' do
    @mock_model.expects(:columns).at_least_once.returns(%w{id mock_col_1 mock_col_2})

    date = Time.utc(2013,04,01)
    date_suffix = date.strftime("%Y%m")

    progress = states('progress').starts_as('existence')

    mock_staging_table = mock
    mock_staging_table.expects(:name).times(2).returns("mock_table_staging")

    @connection.expects(:run_command_with_retry).when(progress.is('existence')).then(progress.is('creation')).with { |args|
      expected = ["SELECT count(tablename) FROM PG_TABLE_DEF WHERE tablename = 'mock_table_#{date_suffix}'"]
      db_command_order_matches(args, expected)
    }.returns([{"count" => 0}])

    @connection.expects(:run_command_with_retry).when(progress.is('creation')).then(progress.is('viewing')).with { |args|
      expected = ["CREATE TABLE mock_table_#{date_suffix} AS SELECT * FROM mock_table_template WHERE 1=0"]
      db_command_order_matches(args, expected)
    }

    @connection.expects(:run_command_with_retry).when(progress.is('viewing')).then(progress.is('upserting')).with { |args|
      expected = ["CREATE OR REPLACE VIEW mock_table_2013 AS SELECT * FROM mock_table_201301 UNION ALL SELECT * FROM mock_table_201302 UNION ALL SELECT * FROM mock_table_201303 UNION ALL SELECT * FROM mock_table_201304"]
      db_command_order_matches(args, expected)
    }

    date_lower_bound = date.beginning_of_month.beginning_of_day.strftime("%F %T")
    date_upper_bound = (date + 1.month).beginning_of_month.beginning_of_day.strftime("%F %T")
    suffixed_mock_table = "mock_table_#{date_suffix}"

    @connection.expects(:run_command_with_retry).when(progress.is('upserting')).then(progress.is('upserted')).with { |args|
      expected = ["UPDATE #{suffixed_mock_table}\n      SET mock_col_1=s.mock_col_1,\nmock_col_2=s.mock_col_2\n        FROM mock_table_staging s\n      WHERE s.id = #{suffixed_mock_table}.id\n        AND s.created_at BETWEEN '#{date_lower_bound}' AND '#{date_upper_bound}'",
                  "INSERT INTO #{suffixed_mock_table}\n        SELECT s.* FROM mock_table_staging s\n          LEFT JOIN #{suffixed_mock_table} t\n          ON s.id = t.id\n        WHERE t.id IS NULL\n        AND s.created_at BETWEEN '#{date_lower_bound}' AND '#{date_upper_bound}'"]
      db_command_order_matches(args, expected)
    }
    

    table = Quasar::AWS::Redshift::DB::TableWithMonthlyPartition.new(@mock_model, @connection, date)
    table.upsert(mock_staging_table)
  end
end
