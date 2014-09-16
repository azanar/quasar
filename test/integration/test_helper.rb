require 'coveralls'

if ENV["ENABLE_SIMPLE_COV"]
  require 'simplecov'
  SimpleCov.start do
    add_group "Lib", "lib"
    add_filter "/test/"
    command_name "Integration Tests"
    formatter SimpleCov::Formatter::MultiFormatter[
      SimpleCov::Formatter::HTMLFormatter,
      Coveralls::SimpleCov::Formatter
    ]
  end
end

require 'test/unit'

ENV["QUASAR_ENV"] = "test"

require 'mocha/setup'
require 'aws'

module TestHelper
  AWS.config(
    :access_key_id => 'ACCESS_KEY_ID',
    :secret_access_key => 'SECRET_ACCESS_KEY',
    :stub_requests => true
  )
end
