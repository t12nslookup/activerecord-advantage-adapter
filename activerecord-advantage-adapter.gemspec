lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

pkg_version = "0.1.3"

Gem::Specification.new do |spec|
  spec.name = "activerecord-advantage-adapter"
  spec.version = pkg_version
  spec.authors = ["Edgar Sherman", "Jon Adams"]
  spec.email = ["advantage@sybase.com", "t12nslookup@googlemail.com"]

  spec.summary = %q{ActiveRecord driver for Advantage}
  spec.description = %q{ActiveRecord driver for the Advantage Database connector}
  spec.homepage = "http://devzone.advantagedatabase.com"
  spec.license = "Apache-2.0"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata["allowed_push_host"] = "https://rubygems.org"

    spec.metadata["homepage_uri"] = spec.homepage
    # Changed to the github project, as this is the actively maintained source, now.
    spec.metadata["source_code_uri"] = "https://github.com/t12nslookup/activerecord-advantage-adapter/"
    spec.metadata["changelog_uri"] = "https://github.com/t12nslookup/activerecord-advantage-adapter/CHANGELOG.md"
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
          "public gem pushes."
  end

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path("..", __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.files = Dir["{test,lib}/**/*",
                   "LICENSE",
                   "README",
                   "activerecord-advantage-adapter.gemspec"]
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.17"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.0"

  spec.add_runtime_dependency "advantage", "~> 0.1", ">= 0.1.2"
  # spec.add_runtime_dependency 'activerecord', '>= 3.2.0'

end
