FROM python:3.10

RUN pip install -U pathway
COPY ./pathway-src/etl.py etl.py
COPY ./pathway-src/reading-results.py reading-results.py

CMD ["python", "-u", "etl.py"]