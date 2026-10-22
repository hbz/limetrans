#! /usr/bin/env ruby

require 'json'

classification_numbers = records = 0
record = datafield = nil

html = { lt: '<', gt: '>', amp: '&', quot: '"' }
  .transform_keys { |k| "&#{k};" }.each_value(&:freeze).freeze

clean_caption = ->(v) {
  v.squeeze!(' ')
  v.gsub!(Regexp.union(*html.keys), html)
  v.gsub!(/&#(\d+);/) { $1.to_i.chr(Encoding::UTF_8) }
  v
}

make_key = ->(*v) { v.compact.join(' - ') }

ARGF.each { |line|
  if line.start_with?('<record>')
    records += 1
    record = Hash.new { |k, v| k[v] = [] }
  elsif record
    if line =~ /<datafield tag="(\d{3})"/
      record[datafield = $1] << []
    elsif datafield
      if line =~ /<subfield code="(\w)">(.+)</
        record[datafield].last << $~.captures
      elsif line.start_with?('</datafield>')
        datafield = nil
      end
    elsif line.start_with?('</record>')
      record['153'].each { |list|
        classification_numbers += 1

        number, ending, caption = %w[a c j].map { |f|
          list.find { |k, v| break v if k == f }
        }

        hierarchy, level = [], {}
        list.each { |k, v|
          case k
            when 'e'
              level[:number] = v
            when 'f'
              level[:ending] = v
            when 'h'
              hierarchy.unshift(level.merge(caption: clean_caption[v]))
              level.clear
          end
        }

        key, value = make_key[number, ending], clean_caption[caption]

        if ENV['INCLUDE_HIERARCHY'] && level = hierarchy.first
          value = [value, make_key[*level.values_at(:number, :ending)]].to_json
        end

        puts "#{key}\u001D#{value}"
      }

      record = datafield = nil
    end
  end
}

warn "#{classification_numbers}/#{records}"
