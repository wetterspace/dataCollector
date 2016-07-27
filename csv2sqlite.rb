require "csv"
require 'date'
require 'sqlite3'
require 'fileutils'
require 'daemons'

filePfad = "/home/wetter/wetterDATA/"

scriptStartDate = DateTime.now
puts "start: " + scriptStartDate.to_s

counterFile = File.read(filePfad + 'csv2sqliteCounter.txt').to_i
globalCounter = File.read(filePfad + 'globalCounter.txt').to_i

# iterates trough the stationID folder which finished from the dataCollector script
for csv2sqliteCounter in counterFile..globalCounter-2 do
	##############################################################
	puts File.directory?(filePfad  + 'csv/' + csv2sqliteCounter.to_s)
	# check if stationID package exists
	if File.directory?(filePfad  + 'csv/' + csv2sqliteCounter.to_s)
		##############################################################
		# create temp file to check if a stationID package finished at the end
		File.new(filePfad + "tmp/" + csv2sqliteCounter.to_s + ".txt", "w+")
		##############################################################
		############################### creates the station database 
		begin
		stationDB = SQLite3::Database.new(filePfad + 'stationDB.db' )
		rows = stationDB.execute <<-SQL  
		  create table IF NOT EXISTS stations  (
		  	stationID Int unique on CONFLICT IGNORE,
		  	Messstation text, 
		  	GeoBreite REAL,
		  	GeoLaenge REAL,
		  	hoehe REAL,
		  	StartDate text,
		  	EndDate text,
		  	Schneehöhe Int DEFAULT 0,
		  	Neuschneehöhe Int DEFAULT 0,
		  	Sonnenscheindauer Int DEFAULT 0,
		  	Windspitze Int DEFAULT 0,
		  	LufttemperaturTagesmittel Int DEFAULT 0,
		  	LufttemperaturTagesmaximum Int DEFAULT 0,
		  	LufttemperaturTagesminimum Int DEFAULT 0,
		  	Niederschlagshöhe  Int DEFAULT 0,
		 	RelativeLuftfeuchte Int DEFAULT 0
		  );
		SQL
		rescue SQLite3::BusyException
		puts "error"
		sleep(3)
		retry
		end
		##############################################################
		# make arrays from one folder with multiple station folders 
		Dir.chdir(filePfad + 'csv/' + csv2sqliteCounter.to_s)
		folders = Dir.glob('*').select {|f| File.directory? f}
		##############################################################
		#iterate through folders array
		for i in 0..folders.count-1
		##############################################################
		# make files in one station folder array 
		files = Dir[filePfad + 'csv/' + csv2sqliteCounter.to_s + '/' + folders[i] + "/*.csv"]
		##############################################################
		# iterate through files
		for file in 0..files.count-1

			csv = CSV.read(files[file], {:col_sep => ";"})

			elementDBName = csv[1][0]
			stationID = (folders[i].to_s).delete "station_"

			##############################################################
			FileUtils.mkdir_p (filePfad + "sql/" + stationID + "/")
			elementDB = SQLite3::Database.open(filePfad + "sql/" + stationID + "/" +   elementDBName.gsub('ö','oe') + '.db' )
			##############################################################
			rows = elementDB.execute <<-SQL
			  create table IF NOT EXISTS data  (
			  StationID Int,
			  Element text,
			  Messstation text,
			  Datum text UNIQUE ON CONFLICT IGNORE,
			  WERT REAL,
			  Einheit text,
			  GeoBreite REAL,
			  GeoLaenge REAL,
			  hoehe REAL
			  );
			SQL
			##############################################################
			for c in 1..csv.count-1
			  	element = csv[c][0]
			    messstation = csv[c][1]
			    datum = csv[c][2]
			    wert = csv[c][3]
			    einheit = csv[c][4]
			    geoBreite = csv[c][5]
			    geoLaenge = csv[c][6]
			    hoehe = csv[c][7]
				elementDB.execute( "INSERT INTO data ( StationID, Element, Messstation, Datum, WERT, Einheit, GeoBreite, GeoLaenge, hoehe ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ? )", [stationID, element, messstation, datum, wert, einheit, geoBreite, geoLaenge, hoehe])
			end
			##############################################################
			startDate = csv[1][2]
			endDate = csv[csv.count-1][2]
			##############################################################
			begin
				stationDB.execute( "INSERT INTO stations ( stationID, Messstation, GeoBreite, GeoLaenge, hoehe) VALUES ( ?, ?, ?, ?, ?)", [stationID, messstation, geoBreite, geoLaenge, hoehe])
				##############################################################
				case element  
					when "Schneehöhe"
						stationDB.execute( "UPDATE stations SET Schneehöhe = 1 WHERE stationID = '" + stationID + "';" )
					when "Neuschneehöhe"
						stationDB.execute( "UPDATE stations SET Neuschneehöhe = 1 WHERE stationID = '" + stationID + "';" )
					when "Sonnenscheindauer"
						stationDB.execute( "UPDATE stations SET Sonnenscheindauer = 1 WHERE stationID = '" + stationID + "';" )
					when "Windspitze"
						stationDB.execute( "UPDATE stations SET Windspitze = 1 WHERE stationID = '" + stationID + "';" )
					when "Lufttemperatur Tagesmittel"
						stationDB.execute( "UPDATE stations SET LufttemperaturTagesmittel = 1 WHERE stationID = '" + stationID + "';" )
					when "Lufttemperatur Tagesmaximum"
						stationDB.execute( "UPDATE stations SET LufttemperaturTagesmaximum = 1 WHERE stationID = '" + stationID + "';" )
					when "Lufttemperatur Tagesminimum"
						stationDB.execute( "UPDATE stations SET LufttemperaturTagesminimum = 1 WHERE stationID = '" + stationID + "';" )
					when "Niederschlagshöhe"
						stationDB.execute( "UPDATE stations SET Niederschlagshöhe = 1 WHERE stationID = '" + stationID + "';" )
					when "Relative Luftfeuchte"
						stationDB.execute( "UPDATE stations SET RelativeLuftfeuchte = 1 WHERE stationID = '" + stationID + "';" )
				end

					rescue SQLite3::BusyException
					puts "error"
					sleep(3)
					retry
			end
			##############################################################
			if file == 0 #this loop only the first time -> performance (set's the start / end date in the stationDB)
				if ((stationDB.execute("select StartDate from stations WHERE stationID = '" + stationID + "';")[0][0] == nil)   || (startDate < (stationDB.execute("select StartDate from stations WHERE stationID = '" + stationID + "';")[0][0])))
					stationDB.execute( "UPDATE stations SET StartDate = '" + startDate + "' WHERE stationID = '" + stationID + "';" )
					puts "startdate: " + stationID.to_s
				end
					if ((stationDB.execute("select EndDate from stations WHERE stationID = '" + stationID + "';")[0][0] == nil)   || (endDate > (stationDB.execute("select EndDate from stations WHERE stationID = '" + stationID + "';")[0][0])))
					stationDB.execute( "UPDATE stations SET EndDate = '" + endDate + "' WHERE stationID = '" + stationID + "';" )
				end
			end
			##############################################################
		end #end files loop
		end # end folder loop
		##############################################################
		#increase the global counter
			File.open(filePfad + "csv2sqliteCounter.txt", 'w') { |file| file.write(csv2sqliteCounter+1) }
			puts "processed " + csv2sqliteCounter.to_s
			FileUtils.rm(filePfad + "tmp/" + csv2sqliteCounter.to_s + ".txt")

			if csv2sqliteCounter.to_i == 574
					dir = filePfad + 'tmp/'
					tmp = Dir[File.join(dir, '**', '*')].count { |file| File.file?(file) }
				while tmp != 0
					sleep(10)
					tmp = Dir[File.join(dir, '**', '*')].count { |file| File.file?(file) }
				end
				#FileUtils.rm_rf(filePfad + "csv/")
				#puts "removed all csv data"
			end
			puts "################# end #############"
		##############################################################
		scriptEndDate = DateTime.now
		puts "end: " + scriptEndDate.to_s
end
end