FROM python:3.10

RUN pip install --extra-index-url https://packages.pathway.com/966431ef6b789140248947273e541114f6679aa3436359bb5d67064b5693047a90ec699d6a pathway
COPY ./pathway-src/process-stream.py process-stream.py

CMD ["python", "-u", "process-stream.py"]