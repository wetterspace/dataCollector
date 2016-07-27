#!/usr/local/bin/ruby
require 'rubygems'
require 'mechanize'
require 'sqlite3'
require "csv"
require 'fileutils'
require 'date'
require 'daemons'

filePfad = "/home/wetter/wetterDATA/"
globalCounter = File.read(filePfad + 'globalCounter.txt')
##############################################################
# date handling
scriptStartDate = DateTime.now
puts "scriptStartDate: " + scriptStartDate.to_s
now = Date.today
sTime = (now - 24)
eTime = (now - 4)
startDate = ((sTime.strftime("%d")).to_i).to_s + "." + sTime.strftime("%m") + "." + sTime.strftime("%Y")
endDate = ((eTime.strftime("%d")).to_i).to_s + "." + eTime.strftime("%m") + "." + eTime.strftime("%Y")
##############################################################

userName = # USERNAME
password = # PASSWORD
num = 12045
fileName = ""
plz = num.to_s

# create zip code array and iterate through the zip codes
zips = CSV.read(filePfad + "zips.csv")
globalCounter = (globalCounter.to_i)
for i in globalCounter..(zips.count-1)
globalCounter = i
plz = zips[i][0]
puts "plz: " + plz
puts "nummer: " + i.to_s 

##############################################################
def element(iii)
	case iii
		when 0
			return  'schneehoehe'
		when 1
			return 'neuschneehoehe'
		when 2
			return  'sonnenscheindauer'
		when 3
			return  'windspitze'
		when 4
			return 'lufttemperatur_tagesmittel'
		when 5
			return 'lufttemperatur_tagesmaximum'
		when 6
			return 'lufttemperatur_tagesminimum'
		when 7
			return 'niederschlagshoehe'
		when 8
			return 'relative_luftfeuchte_tagesmittel'
		else
			return 'default'
	end
end
##############################################################
##############################################################

# iterate through all elements
for elementNR in 0..8

# initialize mechanize library -> log in westeXL
		begin
			agent = Mechanize.new
			page  = agent.get 'https://kunden.dwd.de/weste/xl_login.jsp'
			form = page.form()
			form.username    = userName
			form.password = password
			page = agent.submit(form, form.buttons[0])
				rescue  Mechanize::ResponseCodeError
					puts "error"
				retry
		end
		##############################################################
		##############################################################
		form = page.forms[0]
		form.radiobuttons_with(:name => 'selectedItem')[0].check
		form.radiobuttons_with(:name => 'selectedItem')[0].check
		page = agent.submit(form, form.buttons[0])
		##############################################################
		form = page.forms[1]
		form.fields_with(:name => 'left')[0].value = elementNR
		page = agent.submit(form, form.buttons[0])
		page = agent.submit(form, form.buttons[3])
		##############################################################
		form = page.forms[2]
		form.fields_with(:name => 'startDate')[0].value = startDate
		form.fields_with(:name => 'endDate')[0].value = endDate
		page = agent.submit(form, form.buttons[1])
		##############################################################
		form = page.form_with :name => 'regionSelect'
		form.fields_with(:name => 'location')[0].value = plz
		page = agent.submit(form, form.buttons[0])
		form = page.form_with :name => 'regionSelect'
		page1 = page
		page = agent.submit(form, form.buttons[2])
		##############################################################
		##############################################################
		form = page.forms[0]
		form.fields_with(:name => 'selectedIndex')[0].value = 0
		page = agent.submit(form, form.buttons[0])
		##############################################################
		fileName = element(elementNR)
	if page.links.count > 0
		puts (element(elementNR) + " " + page.links.count.to_s + " stationen")
		##############################################################
		# checking page links for stationIDs 
		page.links.each do |link|
			if link.dom_id != nil
				if page.link_with(:dom_id => link.dom_id) != nil
					# check if the station database exists if not, load the file
					if Dir.glob( filePfad + "csv/" + '*/' + link.dom_id + "/" + fileName + '.csv').empty?

							puts ("                                davon wird: #{link.dom_id}  geladen")
							page.link_with(:dom_id => link.dom_id).click
							##############################################################
							form = page.forms[2]
							page = agent.submit(form, form.buttons[1])
							##############################################################
							##############################################################
							form = page.forms[2]
						if form.checkbox_with(:name => 'csvProduct') != nil
							form.checkbox_with(:name => 'csvProduct').check
						end
						# download the csv script
							response = agent.submit(form, form.buttons[0])
							agent.get('https://kunden.dwd.de/weste/xlproduct/*.csv').save(filePfad + "/csv/" + globalCounter.to_s + '/' + link.dom_id + "/" + fileName + '.csv')
							form = page.forms[2]
							page = agent.submit(form, form.buttons[1])
							page = page1
							form = page.forms[3]
							page = agent.submit(form, form.buttons[2])
					end # if file doesn't exists
				end # dom id not nil
			end # if link dom is not nil
		end # for each link do
	end # if links > 0
end # for 0..8
		############################### count the global counter up
		File.open(filePfad + "globalCounter.txt", 'w') { |file| file.write(i.to_s) }
		##############################################################
		# starting parallelized csv to sqlite script
		
dir = filePfad + 'tmp/'
tmp = Dir[File.join(dir, '**', '*')].count { |file| File.file?(file) }
		# check if the csv2sqlite script finished then start a new daemon (to avoid to much io load on the server)
		if tmp == 0 && globalCounter.to_i > 1
			options = {
			  :multiple => false,
			    :log_output  => true,
			    :monitor     => true,
			    :log_dir     => filePfad + 'log/csv2sqlite/'
			}
			task = Daemons.call(options) do
				exec('ruby ' + filePfad + 'csv2sqlite.rb')
			end
		end
##############################################################
end #zip code 
###############################################################
# set the global counter to end
puts "################# end #############"
scriptEndDate = DateTime.now
puts scriptStartDate 
puts scriptEndDate
##############################################################
# killing the daemons monitor after finishing the script 
 monitorPid = File.read(filePfad + 'bigData.rb_monitor.pid').to_i
 Process.kill('QUIT', monitorPid)
##############################################################
# wait 15 sec after stopping the daemons then set the global counter to 0 
sleep(15)
File.open(filePfad + "globalCounter.txt", 'w') { |file| file.write(577) }