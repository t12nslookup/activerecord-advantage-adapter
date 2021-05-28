
# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).
 
## [0.1.3] - 2021-05-27

  Modified to fix a few bugs, but really to allow "rake db:schema:dump" and "annotate -sk --models" to work, as both of these required access to the indexes stored in the ADS database

### Added
 
### Changed
  
- some rubocop suggestions
  not all rubocop suggestions can be adhered to, as we need to maintain ruby 1.9 compatibility
 
### Fixed
 
- ADS SQL for Indexes
  reconfigured the ADS query to use the built in "get indexes" query, in the same way as the "get columns" def was fixed, so that it works when the data dictionary doesn't work well
- ROWID columns automatically included
  In the "table_structure" def we automatically include the "ROWID" pseudo-column, since ADS generally seems to use this as the primary key for all of it's tables
- Database Column Types
  The additional column types, used to indentify the columns were being created in the wrong location, so weren't being used by the rails activerecord in rails 4+.  The procedure defining these has been moved to the correct location so that column types are identified correctly
 
## [0.1.2] - 2020-11-09
 
### Added
   
- A new modernised gemspec
  Rewrite the gemspec file to create modern gems and allow building for rubygems.org

### Changed

- Modified to work with modern rails
  Lots of changes, but hopefully written in such a way that it will work with rails from versions 2 to 4+

### Fixed

- Many ADS Internal SQL queries
  ADS query syntax relied on SYSTEM tables, which don't always work unless you're in the same specific configuration as the original author.  Changed just enough to get this working to read/write to tables
 
## [0.1.1] - 2012-12-20

last version maintained by Edgar Sherman