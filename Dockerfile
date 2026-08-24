FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN addgroup --system app && adduser --system --ingroup app app

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY main.py ./main.py
COPY src ./src

RUN mkdir -p /data/input /data/output \
    && chown -R app:app /app /data

USER app

ENTRYPOINT ["python", "main.py"]
CMD ["--help"]
