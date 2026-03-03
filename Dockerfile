FROM python:3.13-slim

RUN apt-get update && apt-get install -y \
    chromium \
    fonts-noto-color-emoji fonts-freefont-ttf fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/chromium

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python", "agent.py"]
