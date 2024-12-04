FROM docker.elastic.co/logstash/logstash:7.10.1

# Install the Logstash JDBC output plugin
RUN bin/logstash-plugin install logstash-output-jdbc

# Download and add the PostgreSQL JDBC driver
RUN mkdir -p /usr/share/logstash/logstash-core/lib/jars
RUN curl -L -o /usr/share/logstash/logstash-core/lib/jars/postgresql-42.2.18.jar https://jdbc.postgresql.org/download/postgresql-42.2.18.jar

# Set the log level to debug
ENV LOG_LEVEL=debug