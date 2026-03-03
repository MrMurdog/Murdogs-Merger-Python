FROM python:3.13-slim-trixie
LABEL authors="MrMurdog"

COPY requirements.txt /tmp/requirements.txt

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    make \
    g++ \
    libolm-dev \
  && pip install --no-cache-dir -r /tmp/requirements.txt \
  && apt-get purge -y gcc make g++ \
  && apt-get autoremove -y \
  && rm -rf /var/lib/apt/lists/*

COPY . /app
WORKDIR /app
EXPOSE 9000
VOLUME /app

CMD ["python", "main.py"]