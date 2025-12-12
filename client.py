import grpc
import os
import time
import matplotlib.pyplot as plt
import numpy as np
import protos.dfs_pb2 as pb2
import protos.dfs_pb2_grpc as pb2_grpc

# --- KONFIGURASI ---
TOTAL_FILES = 100       
FILE_SIZE_KB = 500      
OUTPUT_FOLDER = "hasil_output"
JUMLAH_NODE_AKTIF = 3   # Konfigurasi sistem Anda saat ini
# -------------------

def create_dummy_file(filename, size_kb):
    with open(filename, "wb") as f:
        f.write(os.urandom(1024 * size_kb))

def ensure_output_folder():
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

# --- FUNGSI INTI UPLOAD (Bisa Mode Tunggal / Mode Banyak) ---
def upload_process(filename, filesize, file_data, is_sequential=False):
    """
    Jika is_sequential=True -> Hanya kirim ke 1 Node (Simulasi Sekuensial).
    Jika is_sequential=False -> Kirim ke semua target (Simulasi Paralel/Replikasi).
    """
    try:
        # 1. Minta Metadata ke Master
        with grpc.insecure_channel('localhost:50051') as channel:
            stub = pb2_grpc.DFSServiceStub(channel)
            response = stub.RequestUpload(pb2.UploadRequest(filename=filename, filesize=filesize))
            target_nodes = response.target_datanodes

        if not target_nodes: return False

        # --- LOGIKA KUNCI DISINI ---
        if is_sequential:
            # PENTING: Ambil node pertama saja, abaikan sisanya!
            # Ini membuat seolah-olah sistem cuma punya 1 node.
            active_targets = [target_nodes[0]] 
        else:
            # Pakai semua node yang dikasih Master (Replikasi)
            active_targets = target_nodes
        
        success_nodes = []
        
        # 2. Kirim Data ke Target Terpilih
        for node in active_targets:
            try:
                with grpc.insecure_channel(f"{node}:50051") as dn_channel:
                    dn_stub = pb2_grpc.DFSServiceStub(dn_channel)
                    reply = dn_stub.UploadChunk(pb2.ChunkData(filename=filename, data=file_data))
                    if reply.success: success_nodes.append(node)
            except: pass

        # 3. Validasi & Bersih-bersih
        is_success = (len(success_nodes) == len(active_targets))
        
        # Jika Mode Sekuensial, kita hapus lagi filenya agar storage bersih untuk tes utama
        if is_sequential and is_success:
             try:
                with grpc.insecure_channel(f"{active_targets[0]}:50051") as ch:
                    pb2_grpc.DFSServiceStub(ch).DeleteChunk(pb2.ChunkData(filename=filename))
             except: pass

        return is_success

    except Exception as e:
        print(e)
        return False

# --- DASHBOARD GENERATOR ---
def generate_dashboard(indices, latency, throughput, time_seq, time_par, success_count):
    ensure_output_folder()
    
    # Hitung Speedup Real-time
    speedup = time_seq / time_par
    efficiency = speedup / JUMLAH_NODE_AKTIF
    avg_th = sum(throughput)/len(throughput) if throughput else 0

    fig = plt.figure(figsize=(18, 10))
    grid = plt.GridSpec(2, 3, wspace=0.3, hspace=0.3)

    # 1. Latency
    ax1 = fig.add_subplot(grid[0, 0])
    ax1.plot(indices, latency, color='#1f77b4')
    ax1.set_title("Latency History (Paralel)")
    ax1.set_ylabel("ms")
    ax1.grid(True, linestyle='--', alpha=0.5)

    # 2. Throughput
    ax2 = fig.add_subplot(grid[0, 1])
    ax2.plot(indices, throughput, color='#ff7f0e')
    ax2.set_title("Throughput History")
    ax2.set_ylabel("MB/s")
    ax2.grid(True, linestyle='--', alpha=0.5)

    # 3. Speedup Comparison (Bar Chart)
    ax3 = fig.add_subplot(grid[0, 2])
    labels = ['Sequential (1 Node)', f'Parallel ({JUMLAH_NODE_AKTIF} Nodes)']
    times = [time_seq, time_par]
    colors = ['#d62728', '#2ca02c']
    bars = ax3.bar(labels, times, color=colors)
    ax3.set_title(f"Speedup Ratio: {speedup:.2f}x")
    ax3.set_ylabel("Seconds")
    for bar in bars:
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height, f'{height:.2f}s', ha='center', va='bottom')

    # 4. Scalability Projection
    ax4 = fig.add_subplot(grid[1, 0])
    nodes_x = np.linspace(1, JUMLAH_NODE_AKTIF, JUMLAH_NODE_AKTIF)
    ax4.plot(nodes_x, nodes_x, 'k--', label='Ideal')
    ax4.plot([1, JUMLAH_NODE_AKTIF], [1, speedup], 'bo-', label='Real', linewidth=2)
    ax4.set_title(f"Scalability (1 vs {JUMLAH_NODE_AKTIF})")
    ax4.set_xticks(np.arange(1, JUMLAH_NODE_AKTIF + 1, 1))
    ax4.legend()
    ax4.grid(True)

    # 5. KPI Metrics
    ax5 = fig.add_subplot(grid[1, 1])
    metrics = ['Speedup', 'Efficiency (x10)', 'Throughput']
    values = [speedup, efficiency * 10, avg_th]
    ax5.bar(metrics, values, color=['blue', 'orange', 'purple'])
    ax5.set_title("KPI Summary")
    for i, v in enumerate(values): ax5.text(i, v, f'{v:.2f}', ha='center')

    # 6. Summary Text
    ax6 = fig.add_subplot(grid[1, 2]); ax6.axis('off')
    ft_status = "PASSED" if success_count >= TOTAL_FILES * 0.95 else "WARNING"
    txt = (
        f"REPORT\n"
        f"Files: {TOTAL_FILES}\n"
        f"Seq Time: {time_seq:.2f}s\n"
        f"Par Time: {time_par:.2f}s\n"
        f"Speedup: {speedup:.2f}x\n"
        f"Eff: {efficiency:.2f}\n"
        f"Fault Tol: {ft_status}"
    )
    ax6.text(0.1, 0.5, txt, fontsize=12, family='monospace', va='center')

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_FOLDER, "dashboard_performansi.png"))
    plt.close()
    print(f"\n[INFO] Dashboard tersimpan!")

def run():
    # --- FASE 1: TEST SEKUENSIAL (SINGLE NODE) ---
    print(f"\n[PHASE 1] Benchmark Sekuensial ({TOTAL_FILES} File)...")
    print("         (Simulasi: Memaksa upload ke 1 Node saja)")
    
    start_seq = time.time()
    for i in range(TOTAL_FILES): 
        filename = f"bench_{i}.bin"
        create_dummy_file(filename, FILE_SIZE_KB)
        filesize = os.path.getsize(filename)
        with open(filename, "rb") as f: data = f.read()
        
        # Panggil upload dengan mode Sekuensial = True
        upload_process(filename, filesize, data, is_sequential=True)
        
        if os.path.exists(filename): os.remove(filename)
        
    time_seq = time.time() - start_seq
    print(f"✅ Selesai! Waktu Sekuensial: {time_seq:.2f} detik")

    # --- FASE 2: TEST PARALEL (FULL REPLICATION) ---
    print(f"\n[PHASE 2] Pengujian Paralel Utama ({TOTAL_FILES} File)...")
    print("         (Mode: 3 Nodes, Replikasi Aktif)")

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
        
        # Panggil upload dengan mode Sekuensial = False (Paralel)
        is_ok = upload_process(filename, filesize, data, is_sequential=False)
        
        duration = time.time() - t_start
        if is_ok:
            success_count += 1
            indices.append(i+1)
            latencies.append(duration*1000)
            throughputs.append(file_mb/duration)
            print(f"[File {i+1}] ✅ OK ({duration*1000:.1f} ms)")
        else:
            print(f"[File {i+1}] ❌ GAGAL")

        if os.path.exists(filename): os.remove(filename)

    time_par = time.time() - start_par
    print(f"\n✅ Selesai! Waktu Paralel: {time_par:.2f} detik")

    # --- FASE 3: GENERATE GRAFIK ---
    if len(indices) > 0:
        generate_dashboard(indices, latencies, throughputs, time_seq, time_par, success_count)

if __name__ == '__main__':
    run()