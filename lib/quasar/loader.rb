require 'hydrogen'
require 'schlepp'
require 'schlepp/sink/filter/csv'
require 'schlepp/sink/filter/chunker'
require 'converge-pg'

require 'schlepp-sink-fs'


module Quasar
  module Loader
    module_function

    def load(source, model)

      config = {
        :table_name => 'foo',
        :key => 'foo',
        :source => {:file => 'data.csv'},
        :columns => %w{foo bar}
      }

      model = Hydrogen::Model.new(config)

      source = Schlepp::Source::CSV.new(File.new('data.csv','r'))

      sink = Schlepp::Sink::Fs.new(model, :chunk_size => 400, :filters => [Schlepp::Sink::Filter::Csv.new, Schlepp::Sink::Filter::Chunker.new(:chunk_size => 100)])

      res = Schlepp.schlepp(source, sink)

      Converge::Pg.load(model,res)
    end
  end
end
