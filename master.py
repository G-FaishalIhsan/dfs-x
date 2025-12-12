import grpc
from concurrent import futures
import time
import protos.dfs_pb2 as pb2
import protos.dfs_pb2_grpc as pb2_grpc

# Batas waktu toleransi node dianggap mati (detik)
HEARTBEAT_TIMEOUT = 10 

class MasterService(pb2_grpc.DFSServiceServicer):
    def __init__(self):
        self.alive_nodes = {} 
        self.rr_index = 0

    def Heartbeat(self, request, context):
        node_id = request.node_id
        self.alive_nodes[node_id] = time.time()
        return pb2.Reply(success=True, message="Ack")

    def RequestUpload(self, request, context):
        # 1. Filter Node yang Hidup
        current_time = time.time()
        active_nodes = []
        
        for node_id, last_seen in list(self.alive_nodes.items()):
            if current_time - last_seen < HEARTBEAT_TIMEOUT:
                active_nodes.append(node_id)
            else:
                print(f"[Master] ALERT: {node_id} dianggap MATI/DOWN!")
        
        active_nodes.sort() 

        # --- LOGIKA BARU (ADAPTIF) ---
        num_active = len(active_nodes)

        if num_active == 0:
            print("[Master] GAGAL: Tidak ada node aktif sama sekali!")
            return pb2.UploadResponse(filename=request.filename, target_datanodes=[])

        targets = []
        
        if num_active >= 2:
            # Skenario Normal (Replikasi)
            primary = active_nodes[self.rr_index % num_active]
            replica = active_nodes[(self.rr_index + 1) % num_active]
            targets = [primary, replica]
            print(f"[Master] Replikasi -> Primary: {primary}, Replica: {replica}")
        else:
            # Skenario Sekuensial (Single Node) - HANYA 1 Node Hidup
            primary = active_nodes[0]
            targets = [primary]
            print(f"[Master] Single Node (No Replikasi) -> Target: {primary}")

        # Update index Round Robin
        self.rr_index = (self.rr_index + 1) % num_active

        return pb2.UploadResponse(
            filename=request.filename,
            target_datanodes=targets
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_DFSServiceServicer_to_server(MasterService(), server)
    server.add_insecure_port('[::]:50051')
    print("[Master] Server berjalan... Menunggu Heartbeat dari Workers...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()