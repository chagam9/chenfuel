FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# We don't copy main.py here because we will map it via volume in docker-compose
# This allows you to edit the file locally and run it without rebuilding.
# But for production usually you WOULD copy it. For dev, volume is better.
CMD ["python", "main.py"]
