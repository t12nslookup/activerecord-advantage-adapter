print "Using native Advantage Interface\n"
require_dependency 'models/course'
require 'logger'

ActiveRecord::Base.logger = Logger.new("debug.log")

ActiveRecord::Base.configurations = {
  'arunit' => {
    :adapter  => 'advantage',
    :database => 'c:/test/arunit.add',
    :username => 'adssys',
    :password => 
  },
  'arunit2' => {
    :adapter  => 'advantage',
    :database => 'c:/test/arunit2.add',
    :username => 'adssys',
    :password => 
  }
}

ActiveRecord::Base.establish_connection 'arunit'
Course.establish_connection 'arunit2'
