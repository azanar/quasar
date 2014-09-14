require 'hydrogen/table_object'
require 'hydrogen/table_object/collection'
require 'schlepp-aws/sinks/s3/sequencer'
require 'schlepp/sinks/fs/sequencer'
require 'schlepp/sink/sequencer'

require 'pp'

module Quasar
  module Loader
    module_function

    def load(source, model)

      l = Schlepp::AWS::Sink::S3::Sequencer.new(model, :chunk_size => 40000)

      b = Hydrogen::TableObject::Collection::Builder.new(model, l)

      res = Schlepp.schlepp(source, b)

      pp res
      pp res.urls
    end
  end
end
