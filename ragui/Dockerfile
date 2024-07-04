FROM python:3.8-slim

RUN apt-get update && apt-get install -y curl

RUN pip install --upgrade pip

COPY ./requirements.txt /requirements.txt
RUN pip3 install -r /requirements.txt

RUN mkdir -p /app 
COPY /code /app


WORKDIR /app

EXPOSE 8501

CMD ["streamlit", "run", "Rag.py"]