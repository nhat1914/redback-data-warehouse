Structured Dremio Solution - Script

This script is a working version of a pipeline that pulls csv files from github, converts them into pandas dataframe and then feeds them into sqlite to output sql commands to create a table out of it. This is then passed to the specified dremio url in chunks to create a structured sql table of the data.
