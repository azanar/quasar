$LOAD_PATH.push(File.dirname(__FILE__) + '/../lib')

puts $LOAD_PATH

require 'quasar'
require 'aws'

config = {
  :table_name => 'foo',
  :source => {:file => 'data.csv'},
  :columns => %w{foo bar baz}
}

AWS.config(
  access_key_id:     'ACCESS_KEY_ID',
  secret_access_key: 'SECRET_ACCESS_KEY',
  stub_requests:     true #Quasar.env.test?,
)

model = Quasar::Model.new(config)

source = Quasar::Source::CSV.new(File.new('data.csv','r'))

Quasar::Loader.load(source, model)
