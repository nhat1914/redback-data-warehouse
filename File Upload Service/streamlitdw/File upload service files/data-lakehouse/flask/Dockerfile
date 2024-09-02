# Py
FROM python:3.9-slim

# wd
WORKDIR /app

# copy req
COPY requirements.txt /app/

# install packages from req
RUN pip install --no-cache-dir -r requirements.txt

# copy into app
COPY . /app

# port 5000 expose to outside 
EXPOSE 5000

# define env
ENV FLASK_ENV=development

# Run app
CMD ["python", "flaskapi_dw.py"]
