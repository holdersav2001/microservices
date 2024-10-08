FROM python:3.9

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Make wait-for-it.sh executable
RUN chmod +x wait-for-it.sh

# Set PYTHONPATH
ENV PYTHONPATH=/app

CMD ["python", "-m", "main"]