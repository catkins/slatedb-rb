# frozen_string_literal: true

require_relative "lib/slatedb/version"

Gem::Specification.new do |spec|
  spec.name = "slatedb"
  spec.version = SlateDb::VERSION
  spec.authors = ["SlateDB Contributors"]
  spec.email = ["slatedb@example.com"]

  spec.summary = "Ruby bindings for SlateDB"
  spec.description = "A cloud-native embedded key-value store built on object storage"
  spec.homepage = "https://github.com/slatedb/slatedb-rb"
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/slatedb/slatedb-rb"
  spec.metadata["changelog_uri"] = "https://github.com/slatedb/slatedb-rb/blob/main/CHANGELOG.md"

  spec.files = Dir[
    "lib/**/*.rb",
    "ext/**/*.{rb,rs,toml}",
    "Cargo.toml",
    "LICENSE",
    "README.md"
  ]

  spec.bindir = "bin"
  spec.require_paths = ["lib"]
  spec.extensions = ["ext/slatedb/extconf.rb"]

  spec.add_dependency "rb_sys", "~> 0.9"

  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rake-compiler", "~> 1.2"
  spec.add_development_dependency "rspec", "~> 3.12"
  spec.add_development_dependency "rubocop", "~> 1.21"
end
