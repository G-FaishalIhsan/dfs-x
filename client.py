import grpc
import os
import time
import protos.dfs_pb2 as pb2
import protos.dfs_pb2_grpc as pb2_grpc

# --- KONFIGURASI SESUAI SPESIFIKASI ---
TOTAL_FILES = 100       # Jumlah file (Spesifikasi: 100)
FILE_SIZE_KB = 500      # Ukuran per file (500KB x 100 = 50.000KB = 50MB)
# --------------------------------------

def create_dummy_file(filename, size_kb):
    """Membuat file dummy dengan ukuran spesifik"""
    # Menggunakan os.urandom agar isi file acak (mensimulasikan data nyata)
    with open(filename, "wb") as f:
        f.write(os.urandom(1024 * size_kb))

def run():
    print(f"--- MULAI PENGUJIAN UPLOAD ---")
    print(f"Target: {TOTAL_FILES} File")
    print(f"Ukuran per file: {FILE_SIZE_KB} KB")
    print(f"Total Data: {TOTAL_FILES * FILE_SIZE_KB / 1000} MB")
    print("-" * 40)
    
    start_time_global = time.time()
    success_count_total = 0

    # List untuk menyimpan data grafik (opsional untuk laporan)
    throughput_list = []

    for i in range(TOTAL_FILES):
        filename = f"data_skripsi_{i}.bin"
        
        # 1. Buat File Dummy
        create_dummy_file(filename, FILE_SIZE_KB) 
        filesize = os.path.getsize(filename)

        # Mulai timer per file
        start_time_file = time.time()

        try:
            with open(filename, "rb") as f:
                file_data = f.read()

            # 2. Tanya Master (Metadata)
            # Menggunakan 'localhost' karena script ini dijalankan dari container Master
            with grpc.insecure_channel('localhost:50051') as channel:
                stub = pb2_grpc.DFSServiceStub(channel)
                response = stub.RequestUpload(pb2.UploadRequest(filename=filename, filesize=filesize))
                target_nodes = response.target_datanodes

            # 3. Upload ke Data Nodes (Replikasi)
            replicas_success = 0
            for node in target_nodes:
                target_addr = f"{node}:50051"
                try:
                    with grpc.insecure_channel(target_addr) as dn_channel:
                        dn_stub = pb2_grpc.DFSServiceStub(dn_channel)
                        reply = dn_stub.UploadChunk(pb2.ChunkData(filename=filename, data=file_data))
                        if reply.success:
                            replicas_success += 1
                except Exception as e:
                    print(f"   [!] Gagal koneksi ke {node}")

            # 4. Consistency Check
            # Dianggap sukses jika SEMUA replika (Primary & Replica) berhasil
            if replicas_success == len(target_nodes):
                print(f"[File {i+1}/{TOTAL_FILES}] OK -> Nodes {target_nodes}")
                success_count_total += 1
            else:
                print(f"[File {i+1}/{TOTAL_FILES}] PARTIAL/FAIL -> Hanya {replicas_success}/{len(target_nodes)} node")

        except Exception as e:
            print(f"[File {i+1}] ERROR SISTEM: {e}")

        # Hapus file lokal setelah upload (agar container tidak penuh)
        if os.path.exists(filename):
            os.remove(filename)
        
        # Hitung Throughput per file (Opsional untuk data grafik)
        end_time_file = time.time()
        time_taken = end_time_file - start_time_file
        # MB per second
        throughput = (filesize / (1024*1024)) / time_taken 
        throughput_list.append(throughput)

    # --- HASIL AKHIR ---
    end_time_global = time.time()
    total_duration = end_time_global - start_time_global
    
    print("\n" + "="*30)
    print("HASIL PENGUJIAN DFS")
    print("="*30)
    print(f"Total File      : {TOTAL_FILES}")
    print(f"Berhasil Upload : {success_count_total}")
    print(f"Total Waktu     : {total_duration:.2f} detik")
    print(f"Rata-rata Waktu : {total_duration/TOTAL_FILES:.4f} detik/file")
    print(f"Total Ukuran    : {TOTAL_FILES * FILE_SIZE_KB / 1000} MB")
    
    # Hitung Throughput Rata-rata
    avg_throughput = sum(throughput_list) / len(throughput_list)
    print(f"Avg Throughput  : {avg_throughput:.2f} MB/s")
    print("="*30)

if __name__ == '__main__':
    run()