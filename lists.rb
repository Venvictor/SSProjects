require 'rubygems'
require 'nokogiri'
require 'open-uri'

doc = Nokogiri::HTML(open("www.lanternhq.com/courses/37/enrollments"))

docs.css('#story').each do |tst|
  puts tst.inner_text
end

