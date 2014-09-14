require 'hydrogen/table_object'
require 'hydrogen/table_object/collection'
#require 'quasar/schlepp/aws/s3/sequencer'
require 'schlepp/sinks/fs/sequencer'
require 'schlepp/sink/sequencer'

require 'pp'

module Quasar
  module Loader
    module_function

    def load(source, model)

      l = Schlepp::Sink::Fs::Sequencer.new(model, :chunk_size => 40000)

      res = Schlepp.schlepp(source, l)

      #pp res
      unless res.kind_of?(Hydrogen::TableObject::Collection)
        raise "Expected TableObject::Collection, got #{res.class}"
      end
      puts res.path

      aws = Hydrogen::AWS::TableObject.new(res)
      pp aws
      puts aws.path
      puts aws.url
    end

    def write(data)
    end

    def push
    end
  end
end
