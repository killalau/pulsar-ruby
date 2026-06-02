# frozen_string_literal: true

ENV["RUBOCOP_CACHE_ROOT"] ||= File.expand_path(".bundle/rubocop_cache", __dir__)

require "rspec/core/rake_task"
require "rubocop/rake_task"

RSpec::Core::RakeTask.new(:spec)
RuboCop::RakeTask.new(:rubocop)

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

desc "Run lint and test checks used by the pre-push hook"
task verify: %i[rubocop spec]

task default: :spec
