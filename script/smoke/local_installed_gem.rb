# frozen_string_literal: true

require 'open3'
require 'rubygems'
require 'tmpdir'

ROOT = File.expand_path('../..', __dir__)
GEM_NAME = 'pulsar-ruby'
SERVICE_URL = ENV.fetch('PULSAR_SERVICE_URL', 'pulsar://127.0.0.1:6650')

def run!(*command, chdir: ROOT, env: {})
  puts "+ #{command.join(' ')}"
  stdout, stderr, status = Open3.capture3(env, *command, chdir: chdir)
  puts stdout unless stdout.empty?
  warn stderr unless stderr.empty?
  return if status.success?

  abort "#{command.join(' ')} failed with status #{status.exitstatus}"
end

def built_gem_path
  run!('gem', 'build', 'pulsar-ruby.gemspec')
  versions = Dir[File.join(ROOT, "#{GEM_NAME}-*.gem")]
  abort 'gem build did not produce a pulsar-ruby gem' if versions.empty?

  versions.max_by { |path| File.mtime(path) }
end

def write_smoke_app(app_dir)
  File.write(
    File.join(app_dir, 'smoke.rb'),
    <<~RUBY
      # frozen_string_literal: true

      require 'pulsar'

      topic = "persistent://public/default/ruby-smoke-\#{Time.now.to_i}-\#{rand(1000)}"

      Pulsar::Client.open(#{SERVICE_URL.inspect}, operation_timeout: 5, connection_timeout: 5) do |client|
        producer = client.producer(topic: topic)
        consumer = client.consumer(topic: topic, subscription: 'ruby-smoke')

        message_id = producer.send('smoke-message', timeout: 5)
        message = consumer.receive(timeout: 5)
        consumer.ack(message)

        raise "unexpected payload: \#{message.payload.inspect}" unless message.payload == 'smoke-message'
        raise "unexpected message id: \#{message.message_id.inspect}" unless message.message_id == message_id
      end

      puts 'smoke test passed'
    RUBY
  )
end

gem_path = built_gem_path

Dir.mktmpdir('pulsar-ruby-smoke-') do |app_dir|
  gem_home = File.join(app_dir, 'gems')
  gem_env = {
    'GEM_HOME' => gem_home,
    'GEM_PATH' => ([gem_home] + Gem.path).join(File::PATH_SEPARATOR)
  }

  write_smoke_app(app_dir)
  run!('gem', 'install', '--local', '--ignore-dependencies', '--no-document', gem_path, env: gem_env)
  run!('ruby', 'smoke.rb', chdir: app_dir, env: gem_env)
end
