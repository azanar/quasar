require 'quasar/table_object'
#require 'quasar/schlepp/aws/s3/sequencer'
require 'quasar/schlepp/sink/fs/sequencer'
require 'quasar/schlepp/sink/sequencer'

module Quasar
  module Loader
    module_function

    def load(source, model)

      to = Quasar::TableObject.new(model)

      l = Quasar::Schlepp::Sink::Fs::Sequencer.new(to, :chunk_size => 40000)

      out = 10000.times.map do |x|
        "FOO|BAR|BAZ|#{x}\n"
      end

      l.write(out)

      l.finalize

      l.sequence
    end

    def write(data)
    end

    def push
    end
  end
end
