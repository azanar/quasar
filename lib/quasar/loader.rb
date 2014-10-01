require 'hydrogen'
require 'schlepp'
require 'converge-pg'

require 'schlepp/sinks/fs'


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

      l = Schlepp::Sink::Fs::Sequencer.new(model, :chunk_size => 40000)

      res = Schlepp.schlepp(source, l)

      Converge::Pg.load(model,res)
    end
  end
end
