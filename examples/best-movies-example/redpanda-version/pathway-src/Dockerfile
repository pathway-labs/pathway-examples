FROM python:3.10

RUN pip install -U --extra-index-url https://packages.pathway.com/966431ef6ba pathway
COPY ./pathway-src/process-stream.py process-stream.py

CMD ["python", "-u", "process-stream.py"]