#! /usr/bin/env ruby

require 'nokogiri'

dir  = ENV['OUTPUT_DIRECTORY'] || 'src/main/resources/standardnumber'
path = File.join(dir, 'RangeMessage.%s')

File.open(path % :csv, 'w') { |f|
  Nokogiri.XML(File.read(path % :xml)).root.xpath('/ISBNRangeMessage/RegistrationGroups/Group').each { |node|
    prefix, group = node.at_xpath('Prefix').inner_text.split('-')

    node.xpath('Rules/Rule').each { |rule|
      length = rule.at_xpath('Length').inner_text.to_i
      next unless length > 0

      range = rule.at_xpath('Range').inner_text.split('-')
      f.puts [prefix, group, *range.map { |r| r[0, length] }].join(',')
    }
  }
}
