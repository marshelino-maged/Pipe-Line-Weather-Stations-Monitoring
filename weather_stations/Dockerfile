FROM python:3.9-alpine

WORKDIR /app

COPY . /app

# RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "weather_station.py"]
