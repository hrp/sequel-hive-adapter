# sequel-hive-adapter

A Hadoop Hive adapter for Sequel. Uses [rbhive](https://github.com/forward/rbhive) and [thrift](http://thrift.apache.org/).

## Installation

` gem install sequel-hive-adapter`
  
## Usage
```ruby
require 'sequel'
DB = Sequel.connect("hive://localhost")
DB[:table_name].where(date: "2011-05-05").limit(1)
```

Also from the command-line:

```
$ sequel "hive://localhost"
Connecting to localhost on port 10000
Your database is stored in DB...
DB.tables
Executing Hive Query: SHOW TABLES
=> [[:table1,:table2,…]]
```

## Contributing to sequel-hive-adapter
 
* Check out the latest master to make sure the feature hasn't been implemented or the bug hasn't been fixed yet
* Check out the issue tracker to make sure someone already hasn't requested it and/or contributed it
* Fork the project
* Start a feature/bugfix branch
* Commit and push until you are happy with your contribution
* Make sure to add tests for it. This is important so I don't break it in a future version unintentionally.
* Please try not to mess with the Rakefile, version, or history. If you want to have your own version, or is otherwise necessary, that is fine, but please isolate to its own commit so I can cherry-pick around it.

## Online

* [RubyGems.org](http://rubygems.org/gems/sequel-hive-adapter)


## Copyright

Copyright (c) 2011. See LICENSE.txt for
further details.

