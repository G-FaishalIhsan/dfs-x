import grpc
from concurrent import futures
import time
import protos.dfs_pb2 as pb2
import protos.dfs_pb2_grpc as pb2_grpc

# Batas waktu toleransi node dianggap mati (detik)
HEARTBEAT_TIMEOUT = 10 

class MasterService(pb2_grpc.DFSServiceServicer):
    def __init__(self):
        # Dictionary untuk menyimpan kapan terakhir node lapor
        # Format: {"datanode-1": timestamp, "datanode-2": timestamp}
        self.alive_nodes = {} 
        self.rr_index = 0

    def Heartbeat(self, request, context):
        """Menerima sinyal kehidupan dari Data Node"""
        node_id = request.node_id
        # Simpan waktu sekarang
        self.alive_nodes[node_id] = time.time()
        # print(f"[Heartbeat] {node_id} is alive.") # Uncomment untuk debug
        return pb2.Reply(success=True, message="Ack")

    def RequestUpload(self, request, context):
        # 1. Filter Node yang Hidup (Fault Detection)
        current_time = time.time()
        active_nodes = []
        
        # Cek setiap node, apakah heartbeat terakhir < 10 detik yang lalu?
        for node_id, last_seen in list(self.alive_nodes.items()):
            if current_time - last_seen < HEARTBEAT_TIMEOUT:
                active_nodes.append(node_id)
            else:
                print(f"[Master] ALERT: {node_id} dianggap MATI/DOWN!")
        
        # Sort agar urutan konsisten
        active_nodes.sort() 

        if len(active_nodes) < 2:
            print("[Master] GAGAL: Tidak cukup node aktif untuk replikasi!")
            return pb2.UploadResponse(filename=request.filename, target_datanodes=[])

        # 2. Logika Distribusi (Round Robin pada Node Aktif)
        primary = active_nodes[self.rr_index % len(active_nodes)]
        replica = active_nodes[(self.rr_index + 1) % len(active_nodes)]
        
        # Update index
        self.rr_index = (self.rr_index + 1) % len(active_nodes)

        print(f"[Master] Assign -> Primary: {primary}, Replica: {replica} (Aktif: {len(active_nodes)})")

        return pb2.UploadResponse(
            filename=request.filename,
            target_datanodes=[primary, replica]
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