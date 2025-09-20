
FROM python:3.13-slim

WORKDIR /usr/src/consumer
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

ENV PYTHONPATH=/usr/src/consumer

CMD ["python", "-u", "-m", "src.run"]
