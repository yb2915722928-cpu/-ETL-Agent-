FROM python:3.12.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --index-url https://mirrors.aliyun.com/pypi/simple/ -r requirements.txt
COPY app app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
