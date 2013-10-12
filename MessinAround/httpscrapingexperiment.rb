require 'rubygems'
require 'nokogiri'
require 'open-uri'

require 'net/http'

doc = Nokogiri::HTML(File.open("WebDevenrollments.html"))
a = []
enroll = []
i= 1
doc.css('li').each do |link|
a << link.inner_html
i + 1
end

i = 0
i = a.count
i.times do |line|
txt =  a[line].sub('<a href=','')
txt =  txt.to_s.sub('><img alt="Gravatar-140" class="avatar" src=',', ')
txt =  txt.to_s.sub(' title=',', ')
txt =  txt.to_s.sub(' width="300"></a>
<br>
',', ')
txt =  txt.to_s.gsub('
','')
txt =  txt.to_s.sub('\\','')
txt =  txt.to_s.sub('\"','')
txt =  txt.to_s.sub('><',',')
txt =  txt.to_s.gsub('"','')
enroll << "#{txt.chomp}"

end

ii = enroll.count
ii.times do |cell|
test = enroll[cell]
test =  test.split(/\,\s/)
test
# test = test.to_s.gsub('"','')
# test = test.to_s.gsub('\\','')

# puts test.scan(/[, ]+/)
puts test.to_s
end
puts enroll[2]
