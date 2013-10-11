### Introduction

This is a port of the Ruby pg <https://github.com/ged/ruby-pg> postresql driver implemented in pure ruby without 
any native C bindings.

Still a work in progress and currently only tested with ruby 1.9.3 and postgresql 9.1.

It should be a drop in replacement for Ruby pg but some methods have not been implemented.

The synchronous API should be fully functional and useable.

This is a

Use at own risk.

### Requirements

Ruby 1.9.3-p392 or later

PostgreSQL 9.1.x or later

It may work with earlier versions of Ruby/PostgreSQL as well, but they are not tested.

### TODO

Implement async methods
Test performance
Refactor
Consider CI build (travis-ci?)

### Copying

You may redistribute this software under the same terms as Ruby itself; see www.ruby-lang.org/en/LICENSE.txt or the LICENSE file in the source for details. 
