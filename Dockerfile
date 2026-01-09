FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY protocol.py node.py client_cli.py /app/
COPY config/ /app/config/

# opcional: para logs saírem imediatamente
ENV PYTHONUNBUFFERED=1

CMD ["python", "node.py", "/app/config/node1.json"]
