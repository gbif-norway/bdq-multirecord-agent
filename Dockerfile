FROM python:3.12-slim
WORKDIR /app

# Install Python deps
COPY ./requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ .

ENV PYTHONUNBUFFERED=1
ENV PORT=8080

CMD ["python", "main.py"]
