# IBM SPSS Modeler Py Scripting Examples

These are stream execution automation scripting examples I coded when using IBM SPSS Modeler for ETL processes.

## exec_date_sources.py
This script can be used to automate monthly executions of an ETL process. It's main purpose is to save the need to manually update node origins, while also nullify any possible human error on doing so. It has also the built-in functionally to batch-execute several periods, for example when data has to be corrected, the stream has been changed in some manner or the script is applied to a new ETL process stream.

The stream itself is pictured below:

![alt text](https://i.imgur.com/xiP2Fqp.jpg)

This stream is not particularly complex, but remember the scripting code can be used for an arbitrary large amount of data sources and any new or modified ETL process.

## exec_date_aggs.py
This script is very similar **exec_date_sources.py**, but for the purpose of executing ETL processes consisting of time series aggregations of one single data source. In the script below you can see how 12 monthly snapshots are aggregated and transformed in several ways to extract useful data such as min, max, sd, avg, delta, ratio and so on.

Another script functionality is the ability to automatically add individual suffixes to every time-window/transform combination new fields, in order to scale to an arbitrary number of windows, fields, or even different sources without the need to manually declare hundreds or even thousands of fields.

![alt text](https://i.imgur.com/Vfu5P4U.jpg)

## exec_slices.py
This script is focused on being able to loop-execute a stream through a list of values, while using current iteration value to set particular execution parameters. In this case, this script selects records, generates a new field and sets output file based on the iteration value. 

![alt text](https://i.imgur.com/18sALSf.jpg)
