# weatherdata

After browsing the data to understand the contents of the file. My understanding from this is:
1. There are duplicates in the file.
2. Most of the columns were in string format.
3. Change the necessary columns to correct data type to answer the questions from the use case. I.e. change ScreenTemperature from string to decimal, ObservationDate from datetime to date.
4. It seems that the ScreenTemperature  ‘-99.0’ represents the cases when a temperature value is not available for an observation (i.e default value). This is the most frequent value.
5. It also seems that for each given ForecastSiteCode, the temperature is recorded at every hour. This you will see in column ObservationTime where the recording starts at 0 hour and finishes at 23 hours on each day / date (i.e.ObservationDate)
6. There are 10 distinct regions with blank Country names in them. 
      Region                      RowCounts
      Yorkshire & Humber            1320
      Wales                         1376
      Orkney & Shetland             1480
      East Midlands                 1486
      London & South East England   2872
      North East England            2922
      Grampian                      3146
      Highland & Eilean Siar        4384
      North West England            4392
      Northern Ireland              5940

File and the Python script breakdown:
=>CSV files (weather.20160201.csv, weather.20160301.csv): These files were saved in the local folder to read.
=>Parquet file (output): The source files where converted from .csv to parquet and this was saved in the local folder.
=>weather_task.py: Python script where we read the source files and convert them to the parquet format while answering the use case questions.
                   The script is written in Pyspark and Spark SQL which is used to query the data.

Output from the python file:
Hottest day:2016-03-17
Temperature:15.80
Region:Highland & Eilean Siar



