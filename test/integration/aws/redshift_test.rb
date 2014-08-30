require File.expand_path('../../test_helper', __FILE__)

require File.expand_path('../../db_test_helper', __FILE__)

require 'quasar/aws/table_object'
require 'quasar/aws/redshift'

class Quasar::AWS::Redshift::IntegrationTest < Test::Unit::TestCase
  include Quasar::AWS::Redshift::DB::DBTestHelper

  setup do
    @connection = mock

    @mock_models_object_collection = mock

    @mock_model = mock
    @mock_model.expects(:name).at_least_once.returns("mock_models")
    @mock_model.expects(:columns).at_least_once.returns(%w{id mock_col_1 mock_col_2})
    @mock_model.expects(:remove_quotes?).returns(true)

  end

  test 'standard table load starting at S3' do
    @mock_model.expects(:partition?).returns(false)


    progress = states('progress').starts_as('start')

    @connection.expects(:run_command_with_retry).when(progress.is('start')).then(progress.is('staging')).with { |args|
      expected = ["TRUNCATE TABLE mock_models_staging"]
      db_command_order_matches(args, expected)
    }

    @connection.expects(:run_command_with_retry).when(progress.is('staging')).then(progress.is('staged')).with { |args|
      expected = ["COPY mock_models_staging (id,mock_col_1,mock_col_2) FROM 's3://TEST/mock_models/mock_models.psv.gz' CREDENTIALS 'aws_access_key_id=ACCESS_KEY_ID;aws_secret_access_key=SECRET_ACCESS_KEY' REMOVEQUOTES ESCAPE MAXERROR 100 DELIMITER '|' GZIP"]
      db_command_order_matches(args, expected)
    }

    mock_result = mock
    mock_result.expects(:num_tuples).returns(1)
    mock_result.expects(:[]).with(0).returns({"min" => 1.month.ago.to_s, "max" => Time.now.to_s})

    @connection.expects(:run_command_with_retry).when(progress.is('staged')).then(progress.is('sized')).with { |args|
      expected = ["SELECT min(created_at),max(created_at) FROM mock_models_staging"]
      db_command_order_matches(args, expected)
    }.returns(mock_result)

    @connection.expects(:run_command_with_retry).when(progress.is('sized')).then(progress.is('upserted')).with { |args|
      expected = [ "UPDATE mock_models\n      SET mock_col_1=s.mock_col_1,\nmock_col_2=s.mock_col_2\n        FROM mock_models_staging s\n      WHERE s.id = mock_models.id\n",
                   "INSERT INTO mock_models\n        SELECT s.* FROM mock_models_staging s\n          LEFT JOIN mock_models t\n          ON s.id = t.id\n        WHERE t.id IS NULL\n"]
      db_command_order_matches(args, expected)
    }

    @connection.expects(:run_command_with_retry).when(progress.is('upserted')).then(progress.is('analyzed')).with { |args|
      expected = ["VACUUM mock_models",
                    "ANALYZE mock_models"]
      db_command_order_matches(args, expected)
    }

    @connection.expects(:run_command_with_retry).when(progress.is('analyzed')).then(progress.is('done')).with { |args|
      expected = ["TRUNCATE TABLE mock_models_staging"]
      db_command_order_matches(args, expected)
    }

    table_object = Quasar::TableObject.new(@mock_model)
    aws_table_object = Quasar::AWS::TableObject.new(table_object)

    staging_table = Quasar::AWS::Redshift::DB::StagingTable.new(@mock_model)

    staging_loader = staging_table.loader(@connection)
    staging_loader.load(aws_table_object)
  
    collection = Quasar::AWS::Redshift::DB::TableCollection.new(@mock_model, @connection)
    collection_merger = collection.merger

    collection_merger.merge(staging_table)
    staging_loader.finalize
  end

  test 'partitioned table load starting at S3' do
    @mock_model.expects(:partition?).returns(true)

    progress = states('progress').starts_as('start')

    @connection.expects(:run_command_with_retry).when(progress.is('start')).then(progress.is('staging')).with { |args|
      expected = ["TRUNCATE TABLE mock_models_staging"]
      db_command_order_matches(args, expected)
    }

    @connection.expects(:run_command_with_retry).when(progress.is('staging')).then(progress.is('staged')).with { |args|
      expected = ["COPY mock_models_staging (id,mock_col_1,mock_col_2) FROM 's3://TEST/mock_models/mock_models.psv.gz' CREDENTIALS 'aws_access_key_id=ACCESS_KEY_ID;aws_secret_access_key=SECRET_ACCESS_KEY' REMOVEQUOTES ESCAPE MAXERROR 100 DELIMITER '|' GZIP"]
      db_command_order_matches(args, expected)
    }

    min_date = Time.utc(2013,01,15)
    max_date = Time.utc(2013,04,15)

    mock_result = mock
    mock_result.expects(:num_tuples).returns(1)
    mock_result.expects(:[]).with(0).returns({"min" => min_date.to_s, "max" => max_date.to_s})

    expected_date = min_date.beginning_of_month.midnight

    table_suffix = expected_date.strftime("%Y%m")

    @connection.expects(:run_command_with_retry).when(progress.is("staged")).then(progress.is("existence_#{table_suffix}")).with { |args|
      expected = ["SELECT min(created_at),max(created_at) FROM mock_models_staging"]
      db_command_order_matches(args, expected)
    }.returns(mock_result)

    loop do
      next_month = expected_date + 1.month
      start_timestamp = expected_date.strftime("%F %T")
      end_timestamp = next_month.strftime("%F %T")
      table_suffix_local = expected_date.strftime("%Y%m")
      next_month_suffix = next_month.strftime("%Y%m")

      missing_table = next_month >= max_date
      table_count = missing_table ? 0 : 1
      after_existence_state = missing_table ? "creation_#{table_suffix_local}" : "merge_#{table_suffix_local}"

      @connection.expects(:run_command_with_retry).when(progress.is("existence_#{table_suffix_local}")).then(progress.is(after_existence_state)).with { |args| 
        expected = ["SELECT count(tablename) FROM PG_TABLE_DEF WHERE tablename = 'mock_models_#{table_suffix_local}'"]
        db_command_order_matches(args, expected)
      }.returns([{"count" => table_count}])

      if missing_table

        @connection.expects(:run_command_with_retry).when(progress.is("creation_#{table_suffix_local}")).then(progress.is("update_view_#{table_suffix_local}")).with { |args| 
          expected = ["CREATE TABLE mock_models_201304 AS SELECT * FROM mock_models_template WHERE 1=0"]
          db_command_order_matches(args, expected)
        }

        @connection.expects(:run_command_with_retry).when(progress.is("update_view_#{table_suffix_local}")).then(progress.is("merge_#{table_suffix_local}")).with { |args| 
          expected = ["CREATE OR REPLACE VIEW mock_models_2013 AS SELECT * FROM mock_models_201301 UNION ALL SELECT * FROM mock_models_201302 UNION ALL SELECT * FROM mock_models_201303 UNION ALL SELECT * FROM mock_models_201304"]
          db_command_order_matches(args, expected)
        }

      end


      @connection.expects(:run_command_with_retry).when(progress.is("merge_#{table_suffix_local}")).then(progress.is("upserted_#{table_suffix_local}")).with { |args|
        expected = [ "UPDATE mock_models_#{table_suffix_local}\n      SET mock_col_1=s.mock_col_1,\nmock_col_2=s.mock_col_2\n        FROM mock_models_staging s\n      WHERE s.id = mock_models_#{table_suffix_local}.id\nAND s.created_at BETWEEN '#{start_timestamp}' AND '#{end_timestamp}'",
                     "INSERT INTO mock_models_#{table_suffix_local}\n        SELECT s.* FROM mock_models_staging s\n          LEFT JOIN mock_models_#{table_suffix_local} t\n          ON s.id = t.id\n        WHERE t.id IS NULL\nAND s.created_at BETWEEN '#{start_timestamp}' AND '#{end_timestamp}'"]
        db_command_order_matches(args, expected)
      }

      next_state = if next_month >= max_date
                     "truncating"
                   else
                     "existence_#{next_month_suffix}"
                   end

      @connection.expects(:run_command_with_retry).when(progress.is("upserted_#{table_suffix_local}")).then(progress.is(next_state)).with { |args|
        expected = ["VACUUM mock_models_#{table_suffix_local}",
                      "ANALYZE mock_models_#{table_suffix_local}"]
        db_command_order_matches(args, expected)
      }

      if (next_month >= max_date)
        break
      end

      expected_date = next_month
    end

    @connection.expects(:run_command_with_retry).when(progress.is("truncating")).then(progress.is('done')).with { |args|
      expected = ["TRUNCATE TABLE mock_models_staging"]
      db_command_order_matches(args, expected)
    }

    table_object = Quasar::TableObject.new(@mock_model)
    aws_table_object = Quasar::AWS::TableObject.new(table_object)

    staging_table = Quasar::AWS::Redshift::DB::StagingTable.new(@mock_model)

    staging_loader = staging_table.loader(@connection)
    staging_loader.load(aws_table_object)
  
    collection = Quasar::AWS::Redshift::DB::TableCollection.new(@mock_model, @connection)
    collection_merger = collection.merger

    collection_merger.merge(staging_table)
    staging_loader.finalize
  end

end
