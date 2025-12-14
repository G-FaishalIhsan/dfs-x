import grpc
from concurrent import futures
import os
import time
import threading
import protos.dfs_pb2 as pb2
import protos.dfs_pb2_grpc as pb2_grpc

STORAGE_PATH = "/data"
NODE_ID = os.getenv("HOSTNAME", "datanode-unknown")

class DataNodeService(pb2_grpc.DFSServiceServicer):
    def UploadChunk(self, request, context):
        print(f"[{NODE_ID}] Terima file: {request.filename}")
        filepath = os.path.join(STORAGE_PATH, request.filename)
        try:
            with open(filepath, "wb") as f:
                f.write(request.data)
            return pb2.Reply(success=True, message="Disimpan")
        except Exception as e:
            return pb2.Reply(success=False, message=str(e))

    # Fungsi Rollback untuk menjaga Konsistensi
    def DeleteChunk(self, request, context):
        print(f"[{NODE_ID}] ROLLBACK: Menghapus {request.filename}...")
        filepath = os.path.join(STORAGE_PATH, request.filename)
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
                return pb2.Reply(success=True, message="File dihapus (Rollback sukses)")
            else:
                return pb2.Reply(success=True, message="File sudah tidak ada")
        except Exception as e:
            return pb2.Reply(success=False, message=str(e))

def send_heartbeat():
    time.sleep(5)
    while True:
        try:
            with grpc.insecure_channel('master-node:50051') as channel:
                stub = pb2_grpc.DFSServiceStub(channel)
                stub.Heartbeat(pb2.NodeStatus(node_id=NODE_ID, port="50051"))
        except Exception as e:
            print(f"[{NODE_ID}] Gagal Heartbeat: {e}")
        time.sleep(5)

def serve():
    if not os.path.exists(STORAGE_PATH):
        os.makedirs(STORAGE_PATH)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_DFSServiceServicer_to_server(DataNodeService(), server)
    server.add_insecure_port('[::]:50051')
    t = threading.Thread(target=send_heartbeat, daemon=True)
    t.start()
    print(f"[{NODE_ID}] Siap di port 50051. Heartbeat aktif.")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()