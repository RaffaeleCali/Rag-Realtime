FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
RUN pip install pandas 
COPY . .
RUN chmod -R 755 /app
CMD ["python", "server.py"]
