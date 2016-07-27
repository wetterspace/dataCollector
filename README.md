# dataCollector

this script loads csv files from dwd, stores them in sqlite files and creates a weather station database.


1. starting the daemons script
	- this starts the dataCollector script in the background

2. the dataCollector script sets the starttime to 24 days before now and end date 4 days before now.
3. takes the csv file with 576 zip codes allocated all over germany and starts iterating through them
4. iterates while that through the 9 elements
	- and then through all reachable stations 
	- then downloads for every element and every station one csv file, containing the data of the preconfigured period. store it to a folder named by the station which is stored in a folder named by the line number of the zip code in the zips.csv file

	e.g.:

	zip code 12045 is the first zip code in the file, and one of the station ids is 399 then the structure is:

	/csv/1/399/Sonnenscheindauer.sqlite and so on (see the scheme below)


5. dataCollector script generates 575 folders,
	- they contain folders of single stations, 
		- they contain csv files named by the 9 elements


6. when a station cluster finished downloading, it starts the csv2sqlite script, if it's not already running

7. the csv2sqlite script then starts with the station cluster folder, iterates through all station folder, takes every single csv file and write it to a sqlite database and save it to a folder named by the station. e.g.: 


>
	cluster -> 1
		stationID -> 399
					element -> Sonnenscheindauer.sqlite 
					element -> Schneehöhe.sqlite 
					element -> Neuschneehöhe.sqlite 
					element -> Windspitze.sqlite 
					element -> LufttemperaturTagesmittel.sqlite 
					element -> LufttemperaturTagesmaximum.sqlite 
					element -> LufttemperaturTagesminimum.sqlite 
					element -> Niederschlagshöhe.sqlite 
					element -> RelativeLuftfeuchte.sqlite 
		stationID -> 411
					element -> Sonnenscheindauer.sqlite 
					element -> Schneehöhe.sqlite 
					element -> Neuschneehöhe.sqlite 
		etc.

8 . then it opens the stationID database, writes(updates) start/end dates and existing elements of a station

9 . when the script finished a cluster, it checks if there is a new completed cluster and startes with that or stops.