FROM python:2.7
RUN apt-get update && apt-get install unzip
ADD . /code/
WORKDIR /code/
RUN pip install -r requirements.txt

CMD ["python", "/code/nppes-cron.py"]
