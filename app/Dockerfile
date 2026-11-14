FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 CASS_DRIVER_NO_CYTHON=1
WORKDIR /workspace
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["sleep", "infinity"]
