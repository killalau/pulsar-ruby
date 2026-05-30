# frozen_string_literal: true

RSpec.describe "protobuf generation task" do
  it "defines proto:generate" do
    load File.expand_path("../../../Rakefile", __dir__)

    expect(Rake::Task.task_defined?("proto:generate")).to be(true)
  end
end
