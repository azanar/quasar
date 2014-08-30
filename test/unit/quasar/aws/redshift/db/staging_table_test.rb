require File.expand_path('../../../../../test_helper', __FILE__)

require 'quasar/aws/redshift/db/staging_table'

class Quasar::AWS::Redshift::DB::StagingTableTest < Test::Unit::TestCase

  test 'date_range' do 
    pend
  end

end

class Quasar::AWS::Redshift::DB::StagingTable::LoaderTest < Test::Unit::TestCase
  setup do
    @test_host = "redshift-test.sjc.carrel.org"
    @test_table = "mock_table"
    @test_staging_table = "#{@test_table}_staging"

    @connection = mock
    @mock_model = mock
    @mock_model.expects(:name).at_least_once.returns(@test_table)
    @mock_model.expects(:columns).at_least_once.returns(%w{id mock_col_1 mock_col_2})
    @staging_table = Quasar::AWS::Redshift::DB::StagingTable.new(@mock_model)
  end

  def db_command_order_matches(lines, expected)
    lines = lines.split(/;\n/).map(&:strip).reject(&:empty?)
    expected.length == lines.length and expected.zip(lines).all? do |elt| 
      line = elt[0].split(/\n/).map(&:strip).join(" ")
      expected = elt[1].split(/\n/).map(&:strip).join(" ")
      line == expected
    end
  end

  test 'load no remove quote table' do
    @mock_model.expects(:remove_quotes?).returns(false)


    _object_collection = mock
    _object_collection.expects(:s3_url).returns("s3://#{@test_host}/#{@test_table}")

    progress = states('progress').starts_as('start')

    @connection.expects(:run_command_with_retry).when(progress.is('start')).then(progress.is('truncated')).with { |args|
      expected = ["TRUNCATE TABLE #{@test_staging_table}"]
      db_command_order_matches(args, expected)
    }

    @connection.expects(:run_command_with_retry).when(progress.is('truncated')).then(progress.is('staged')).with { |args|
      expected = ["COPY #{@test_staging_table} (id,mock_col_1,mock_col_2) FROM 's3://#{@test_host}/#{@test_table}' CREDENTIALS 'aws_access_key_id=ACCESS_KEY_ID;aws_secret_access_key=SECRET_ACCESS_KEY'  ESCAPE MAXERROR 100 DELIMITER '|' GZIP"]
      db_command_order_matches(args, expected)
    }

    @connection.expects(:run_command_with_retry).when(progress.is('staged')).then(progress.is('done')).with { |args|
      expected = ["TRUNCATE TABLE #{@test_staging_table}"]
      db_command_order_matches(args, expected)
    }

    loader = @staging_table.loader(@connection)
    loader.load(_object_collection)
    loader.finalize
  end

  test 'load' do 
    @mock_model.expects(:remove_quotes?).returns(true)

    _object_collection = mock
    _object_collection.expects(:s3_url).returns("s3://#{@test_host}/#{@test_table}")

    progress = states('progress').starts_as('start')

    @connection.expects(:run_command_with_retry).when(progress.is('start')).then(progress.is('truncated')).with { |args|
      expected = ["TRUNCATE TABLE #{@test_staging_table}"]
      db_command_order_matches(args, expected)
    }

    @connection.expects(:run_command_with_retry).when(progress.is('truncated')).then(progress.is('staged')).with { |args|
      expected = ["COPY #{@test_staging_table} (id,mock_col_1,mock_col_2) FROM 's3://#{@test_host}/#{@test_table}' CREDENTIALS 'aws_access_key_id=ACCESS_KEY_ID;aws_secret_access_key=SECRET_ACCESS_KEY' REMOVEQUOTES ESCAPE MAXERROR 100 DELIMITER '|' GZIP"]
      db_command_order_matches(args, expected)
    }

    @connection.expects(:run_command_with_retry).when(progress.is('staged')).then(progress.is('done')).with { |args|
      expected = ["TRUNCATE TABLE #{@test_staging_table}"]
      db_command_order_matches(args, expected)
    }

    loader = @staging_table.loader(@connection)
    loader.load(_object_collection)
    loader.finalize

  end
end
