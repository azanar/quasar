require 'hydrogen/table_object/collection/builder'
#require 'schlepp-aws/sinks/s3/sequencer'
require 'schlepp/sinks/fs/sequencer'
#require 'schlepp/sink/sequencer'
require 'converge'

module Quasar
  module Loader
    module_function

    def load(source, model)

      #l = Schlepp::AWS::Sink::S3::Sequencer.new(model, :chunk_size => 40000)
      l = Schlepp::Sink::Fs::Sequencer.new(model, :chunk_size => 40000)

      b = Hydrogen::TableObject::Collection::Builder.new(model, l)

      res = Schlepp.schlepp(source, b)

      Converge.load(res)
    end
  end
end
