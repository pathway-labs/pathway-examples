FROM python:3.10

RUN pip install kafka-python
RUN pip install pytz
COPY ./producer-src/create-stream.py create-stream.py

CMD ["python", "-u", "create-stream.py"]