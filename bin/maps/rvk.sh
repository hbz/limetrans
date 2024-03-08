#! /bin/bash

set -e

d="${OUTPUT_DIRECTORY:-src/main/resources/transformation/maps}"
p="$d/rvk"

e=lmdb
f="$p.$e"
t="$p.$$.$e"

curl --no-progress-meter\
  'https://rvk.uni-regensburg.de/downloads/FachA-Z_2023_2.csv.gz' |\
  gunzip -c |\
  ruby -e '
    html = { lt: "<", gt: ">", amp: "&", quot: "\"" }.transform_keys { |k| "&#{k};" }

    ARGF.each { |r|
      r.chomp!

      r.sub!(/\A\|*/, "")
      r.sub!(/\|* *\z/, "")

      k, v = r.split("|", 2)
      next unless k && v

      v.gsub!(/(?<=[^ |])\|(?=[^ |])/, " ")
      v.gsub!(Regexp.union(*html.keys), html)
      v.gsub!(/&#(\d+);/) { $1.to_i.chr(Encoding::UTF_8) }

      puts "#{k}\u001D#{v}"
    }
  ' |\
  ./gradlew execLmdb --args="$t"

[ -s "$t" ] && mv "$t" "$f"

./gradlew execLmdb --args="$f"
