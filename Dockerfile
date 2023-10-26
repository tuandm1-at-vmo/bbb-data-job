FROM python:3.10-slim

RUN apt-get update && \
    apt-get install systemd cron -y && \
    apt-get clean && \
    pip install --upgrade pip && \
    systemctl enable cron

WORKDIR /bicyclebluebook
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY app ./app
COPY cron/bicyclebluebook /etc/cron.d/
RUN chmod 0644 /etc/cron.d/bicyclebluebook
RUN crontab /etc/cron.d/bicyclebluebook

RUN python -m app.main
CMD cron -f