import grpc
import os
import time
import matplotlib.pyplot as plt
import numpy as np
import protos.dfs_pb2 as pb2
import protos.dfs_pb2_grpc as pb2_grpc

# --- KONFIGURASI UTAMA ---
TOTAL_FILES = 100       
FILE_SIZE_KB = 500      
OUTPUT_FOLDER = "hasil_output"

# Set ke 8 Node
JUMLAH_NODE_AKTIF = 3
# -------------------------

def create_dummy_file(filename, size_kb):
    with open(filename, "wb") as f:
        f.write(os.urandom(1024 * size_kb))

def ensure_output_folder():
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

# --- FUNGSI INTI UPLOAD ---
def upload_process(filename, filesize, file_data, is_sequential=False):
    try:
        # 1. Minta Metadata ke Master
        with grpc.insecure_channel('localhost:50051') as channel:
            stub = pb2_grpc.DFSServiceStub(channel)
            response = stub.RequestUpload(pb2.UploadRequest(filename=filename, filesize=filesize))
            target_nodes = response.target_datanodes

        if not target_nodes: return False

        # LOGIKA TARGET:
        # Jika Sequential -> Paksa ambil 1 node saja (Target[0])
        # Jika Paralel -> Paksa ambil semua node yang dikasih Master
        if is_sequential:
            active_targets = [target_nodes[0]] 
        else:
            active_targets = target_nodes
        
        success_nodes = []
        
        # 2. Kirim Data ke Target
        for node in active_targets:
            try:
                with grpc.insecure_channel(f"{node}:50051") as dn_channel:
                    dn_stub = pb2_grpc.DFSServiceStub(dn_channel)
                    reply = dn_stub.UploadChunk(pb2.ChunkData(filename=filename, data=file_data))
                    if reply.success: success_nodes.append(node)
            except: pass

        # 3. Validasi
        is_success = (len(success_nodes) == len(active_targets))
        
        # Bersih-bersih file benchmark (sequential)
        if is_sequential and is_success:
             try:
                with grpc.insecure_channel(f"{active_targets[0]}:50051") as ch:
                    pb2_grpc.DFSServiceStub(ch).DeleteChunk(pb2.ChunkData(filename=filename))
             except: pass

        return is_success

    except Exception as e:
        print(e)
        return False

# --- DASHBOARD GENERATOR (VISUALISASI 8 NODE) ---
def generate_dashboard(indices, latency, throughput, time_seq, time_par, success_count):
    ensure_output_folder()
    
    # Hitung Speedup & Efisiensi
    speedup = time_seq / time_par
    efficiency = speedup / JUMLAH_NODE_AKTIF
    avg_th = sum(throughput)/len(throughput) if throughput else 0

    fig = plt.figure(figsize=(18, 10))
    grid = plt.GridSpec(2, 3, wspace=0.3, hspace=0.3)

    # 1. Latency History
    ax1 = fig.add_subplot(grid[0, 0])
    ax1.plot(indices, latency, color='#1f77b4', alpha=0.8)
    ax1.set_title("Latency History (Parallel)")
    ax1.set_ylabel("Latency (ms)")
    ax1.grid(True, linestyle='--', alpha=0.5)

    # 2. Throughput History
    ax2 = fig.add_subplot(grid[0, 1])
    ax2.plot(indices, throughput, color='#ff7f0e', alpha=0.8)
    ax2.set_title("Throughput History")
    ax2.set_ylabel("Throughput (MB/s)")
    ax2.grid(True, linestyle='--', alpha=0.5)

    # 3. Speedup Bar Chart (1 vs 8)
    ax3 = fig.add_subplot(grid[0, 2])
    labels = ['Sequential (1 Node)', f'Parallel ({JUMLAH_NODE_AKTIF} Nodes)']
    times = [time_seq, time_par]
    colors = ['#d62728', '#2ca02c'] # Merah vs Hijau
    bars = ax3.bar(labels, times, color=colors)
    ax3.set_title(f"Speedup Ratio: {speedup:.2f}x")
    ax3.set_ylabel("Execution Time (s)")
    for bar in bars:
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height, f'{height:.2f}s', ha='center', va='bottom')

    # 4. Scalability Projection (REVISI: Visualisasi 8 Node)
    ax4 = fig.add_subplot(grid[1, 0])
    
    # Garis Ideal (Linear ke atas)
    ideal_x = np.linspace(1, JUMLAH_NODE_AKTIF, JUMLAH_NODE_AKTIF)
    ax4.plot(ideal_x, ideal_x, 'k--', label='Ideal Linear')
    
    # Garis Real (1 ke 8)
    real_x = [1, JUMLAH_NODE_AKTIF]
    real_y = [1, speedup]
    ax4.plot(real_x, real_y, 'bo-', label='Real Result', linewidth=2)
    
    ax4.set_title(f"Scalability Projection (1 vs {JUMLAH_NODE_AKTIF})")
    ax4.set_xlabel("Number of Nodes")
    ax4.set_ylabel("Speedup Factor")
    
    # [PENTING] Atur Tick Marks agar rapi (1, 2, 4, 8)
    if JUMLAH_NODE_AKTIF == 8:
        ax4.set_xticks([1, 2, 4, 8])
    else:
        ax4.set_xticks(np.arange(1, JUMLAH_NODE_AKTIF + 1, 1))
        
    ax4.legend()
    ax4.grid(True)

    # 5. KPI Summary
    ax5 = fig.add_subplot(grid[1, 1])
    metrics = ['Speedup', 'Efficiency (x10)', 'Avg T-put']
    values = [speedup, efficiency * 10, avg_th]
    ax5.bar(metrics, values, color=['blue', 'orange', 'purple'])
    ax5.set_title("Key Performance Indicators")
    for i, v in enumerate(values): ax5.text(i, v, f'{v:.2f}', ha='center', va='bottom')

    # 6. Text Report
    ax6 = fig.add_subplot(grid[1, 2]); ax6.axis('off')
    ft_status = "PASSED" if success_count >= TOTAL_FILES * 0.95 else "WARNING"
    txt = (
        f"TEST REPORT\n"
        f"--------------------------\n"
        f"Nodes Config: {JUMLAH_NODE_AKTIF} Nodes\n"
        f"Total Files : {TOTAL_FILES}\n"
        f"Seq Time    : {time_seq:.2f} s\n"
        f"Par Time    : {time_par:.2f} s\n"
        f"Speedup     : {speedup:.2f}x\n"
        f"Efficiency  : {efficiency:.2f}\n"
        f"Fault Tol   : {ft_status}"
    )
    ax6.text(0.1, 0.5, txt, fontsize=12, family='monospace', va='center')

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_FOLDER, "dashboard_performansi.png"))
    plt.close()
    print(f"\n[INFO] Dashboard grafik 8 Node tersimpan!")

def run():
    # FASE 1: BENCHMARK SEKUENSIAL
    print(f"\n[PHASE 1] Benchmark Sekuensial ({TOTAL_FILES} File)...")
    start_seq = time.time()
    for i in range(TOTAL_FILES): 
        filename = f"bench_{i}.bin"
        create_dummy_file(filename, FILE_SIZE_KB)
        filesize = os.path.getsize(filename)
        with open(filename, "rb") as f: data = f.read()
        upload_process(filename, filesize, data, is_sequential=True)
        if os.path.exists(filename): os.remove(filename)
    time_seq = time.time() - start_seq
    print(f"✅ Selesai! Waktu Sekuensial: {time_seq:.2f} s")

    # FASE 2: UJI PARALEL 8 NODE
    print(f"\n[PHASE 2] Pengujian Paralel {JUMLAH_NODE_AKTIF} Node ({TOTAL_FILES} File)...")
    start_par = time.time()
    success_count = 0
    indices, latencies, throughputs = [], [], []

    for i in range(TOTAL_FILES):
        filename = f"data_{i}.bin"
        create_dummy_file(filename, FILE_SIZE_KB)
        filesize = os.path.getsize(filename)
        file_mb = filesize / (1024*1024)
        
        t_start = time.time()
        with open(filename, "rb") as f: data = f.read()
        
        is_ok = upload_process(filename, filesize, data, is_sequential=False)
        
        duration = time.time() - t_start
        if is_ok:
            success_count += 1
            indices.append(i+1)
            latencies.append(duration*1000)
            throughputs.append(file_mb/duration)
            print(f"[File {i+1}] ✅ OK")
        
        if os.path.exists(filename): os.remove(filename)

    time_par = time.time() - start_par
    print(f"\n✅ Selesai! Waktu Paralel: {time_par:.2f} s")

    if len(indices) > 0:
        generate_dashboard(indices, latencies, throughputs, time_seq, time_par, success_count)

if __name__ == '__main__':
    run()