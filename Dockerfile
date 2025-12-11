FROM python:3.9-slim

WORKDIR /app

# Copy requirements dan install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy seluruh kode project ke dalam container
COPY . .

# Generate ulang proto di dalam container (untuk memastikan kompatibilitas)
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. protos/dfs.proto

# Command default (bisa di-override lewat docker-compose)
CMD ["python", "master.py"]