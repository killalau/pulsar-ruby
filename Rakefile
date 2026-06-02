# frozen_string_literal: true

require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec)

namespace :spec do
  desc "Run Pulsar standalone integration specs"
  RSpec::Core::RakeTask.new(:integration) do |task|
    ENV["PULSAR_INTEGRATION"] = "1"
    task.pattern = "spec/integration/**/*_spec.rb"
  end
end

namespace :proto do
  desc "Generate Ruby protobuf definitions from proto/PulsarApi.proto"
  task :generate do
    sh "protoc", "--proto_path=proto", "--ruby_out=lib/pulsar/proto", "proto/PulsarApi.proto"
  end
end

task default: :spec
