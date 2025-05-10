# py
FROM python:3.9-slim

# wd
WORKDIR /app

# Install Java, procps
RUN apt-get update && \
    apt-get install -y default-jdk procps && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable and update PATH to solve slit error
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"

# copy
COPY . /app

# install packages
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8501 available outside of vm
EXPOSE 8501

# run file upload service
CMD ["streamlit", "run", "streamlitdw_fe_mt.py"]
