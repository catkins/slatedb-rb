# frozen_string_literal: true

require "bundler/gem_tasks"
require "rspec/core/rake_task"
require "rb_sys/extensiontask"

RSpec::Core::RakeTask.new(:spec)

GEMSPEC = Gem::Specification.load("slatedb.gemspec")

RbSys::ExtensionTask.new("slatedb", GEMSPEC) do |ext|
  ext.lib_dir = "lib/slatedb"
  ext.ext_dir = "ext/slatedb"
end

task default: %i[compile spec]
task test: :spec
