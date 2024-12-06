This core infrastructure folder containers as of 4/12/2024 roughly 80% of the DW team infrastructure.

All services can be seen listed in the .yml file.

Folder Structure:
- app = streamlit file upload service
- db-init = init file for postgres server
- dremio-api = api used to send select sql queries to tables stored in dremio
- flask = flask api used to download files from file upload service
- kibana_config = config file for kibana
- logstash = logstash.conf file crucial to the data provenance pipeline with parsing data from format to format
