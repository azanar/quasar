$LOAD_PATH.push(File.dirname(__FILE__) + '/../lib')

require 'quasar'
require 'aws'


config = {
  :table_name => 'foo',
  :source => {:file => 'data.csv'},
  :columns => %w{foo bar baz}
}

AWS.config(
  access_key_id:     'AKIAJPMXDPOH6JHRJXBQ',
  secret_access_key: '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',
  stub_requests:     Quasar.env.test?,
)

model = Hydrogen::Model.new(config)

source = Schlepp::Source::CSV.new(File.new('data.csv','r'))

Quasar::Loader.load(source, model)
