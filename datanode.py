import grpc
from concurrent import futures
import os
import time
import threading
import protos.dfs_pb2 as pb2
import protos.dfs_pb2_grpc as pb2_grpc

STORAGE_PATH = "/data"

# Ambil nama host dari Environment Variable (diset di docker-compose)
# Jika tidak ada, pakai hostname default
NODE_ID = os.getenv("HOSTNAME", "datanode-unknown")

class DataNodeService(pb2_grpc.DFSServiceServicer):
    def UploadChunk(self, request, context):
        print(f"[{NODE_ID}] Terima file: {request.filename}")
        filepath = os.path.join(STORAGE_PATH, request.filename)
        with open(filepath, "wb") as f:
            f.write(request.data)
        return pb2.Reply(success=True, message="Disimpan")

def send_heartbeat():
    """Mengirim sinyal 'Saya Hidup' ke Master setiap 5 detik"""
    time.sleep(5) # Tunggu sebentar agar Master siap
    while True:
        try:
            with grpc.insecure_channel('master-node:50051') as channel:
                stub = pb2_grpc.DFSServiceStub(channel)
                stub.Heartbeat(pb2.NodeStatus(node_id=NODE_ID, port="50051"))
            # print(f"[{NODE_ID}] Heartbeat terkirim.")
        except Exception as e:
            print(f"[{NODE_ID}] Gagal Heartbeat ke Master: {e}")
        
        time.sleep(5) # Kirim setiap 5 detik

def serve():
    if not os.path.exists(STORAGE_PATH):
        os.makedirs(STORAGE_PATH)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_DFSServiceServicer_to_server(DataNodeService(), server)
    server.add_insecure_port('[::]:50051')
    
    # Jalankan Heartbeat di background (thread terpisah)
    t = threading.Thread(target=send_heartbeat, daemon=True)
    t.start()

    print(f"[{NODE_ID}] Siap di port 50051. Heartbeat aktif.")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()