# py
FROM python:3.9-slim

# wd
WORKDIR /app

# copy
COPY . /app

# install packages
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8501 available outside of vm
EXPOSE 8501

# run file upload service
CMD ["streamlit", "run", "streamlitdw_fe.py"]
