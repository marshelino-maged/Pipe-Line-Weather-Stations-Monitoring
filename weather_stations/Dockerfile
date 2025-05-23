FROM python:3.9-alpine

WORKDIR /app

COPY . /app

RUN apk add --no-cache librdkafka-dev

RUN pip install --no-cache-dir -r requirements.txt

# Expose port 80 (if your app uses networking, otherwise this can be omitted)
# EXPOSE 80

# Run weather_station.py when the container launches
CMD ["python", "weather_station.py"]