"""
MODULE 2: PROCESSING SERVER (RECEIVER)
======================================
- Nhận gói tin frame từ Camera Server qua TCP
- Xóa nền cho từng frame sử dụng Spark
- Lưu các frame thành file ảnh
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import socket
import cv2
import numpy as np
import os
import time
import base64
from background_remover import remove_background

class Config:
    """Cấu hình"""
    LISTEN_HOST = "0.0.0.0"
    LISTEN_PORT = 6100
    OUTPUT_DIR = "output_frames"
    SPARK_APP_NAME = "ProcessingServer_Spark"
    BATCH_DURATION = 1

def decode_base64_to_frame(base64_str):
    """Decode base64 string thành frame numpy array"""
    try:
        img_bytes = base64.b64decode(base64_str)
        nparr = np.frombuffer(img_bytes, np.uint8)
        frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        return frame
    except Exception as e:
        print(f"❌ Lỗi decode: {e}")
        return None

def save_frame_to_file(frame, frame_id, output_dir):
    """Lưu frame thành file ảnh"""
    try:
        filename = os.path.join(output_dir, f"frame_{frame_id:04d}.jpg")
        cv2.imwrite(filename, frame)
        return filename
    except Exception as e:
        print(f"❌ Lỗi lưu file: {e}")
        return None

def main():
    print("=" * 70)
    print("🎬 MODULE 2: PROCESSING SERVER (Spark)")
    print("   Nhận frame → Xóa nền bằng Spark → Lưu file ảnh")
    print("=" * 70)
    
    # ========== TẠO THƯ MỤC OUTPUT ==========
    if not os.path.exists(Config.OUTPUT_DIR):
        os.makedirs(Config.OUTPUT_DIR)
        print(f"\n📁 Tạo thư mục output: {Config.OUTPUT_DIR}")
    else:
        # Xóa file cũ
        for f in os.listdir(Config.OUTPUT_DIR):
            os.remove(os.path.join(Config.OUTPUT_DIR, f))
        print(f"\n📁 Xóa file cũ trong: {Config.OUTPUT_DIR}")
    
    # ========== KHỞI TẠO SPARK ==========
    sc = SparkContext(appName=Config.SPARK_APP_NAME, master="local[*]")
    sc.setLogLevel("ERROR")
    
    print(f"\n✅ Spark Context khởi tạo: {Config.SPARK_APP_NAME}")
    print(f"   Parallelism: {sc.defaultParallelism}")
    
    # Khởi tạo Spark Streaming
    ssc = StreamingContext(sc, Config.BATCH_DURATION)
    print(f"✅ Spark Streaming khởi tạo (batch: {Config.BATCH_DURATION}s)")
    
    # ========== KHỞI TẠO TCP SERVER ==========
    print(f"\n🔗 Khởi tạo TCP Server ({Config.LISTEN_HOST}:{Config.LISTEN_PORT})...")
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server_socket.bind((Config.LISTEN_HOST, Config.LISTEN_PORT))
        server_socket.listen(1)
        print(f"✅ Server đang lắng nghe trên port {Config.LISTEN_PORT}")
        print("⏳ Chờ Camera Server kết nối...")
    except Exception as e:
        print(f"❌ Lỗi bind: {e}")
        sc.stop()
        return
    
    # Chờ kết nối
    try:
        conn, addr = server_socket.accept()
        print(f"✅ Camera Server kết nối: {addr}")
    except Exception as e:
        print(f"❌ Lỗi accept: {e}")
        server_socket.close()
        sc.stop()
        return
    
    # ========== NHẬN VÀ XỬ LÝ FRAME ==========
    print("\n📥 Bắt đầu nhận frame...")
    print("-" * 70)
    
    received_frames = []
    buffer = ""
    
    try:
        while True:
            # Nhận dữ liệu
            data = conn.recv(65536)
            if not data:
                break
            
            buffer += data.decode('utf-8')
            
            # Xử lý từng dòng (mỗi frame là 1 dòng)
            while '\n' in buffer:
                line, buffer = buffer.split('\n', 1)
                
                if line == "END":
                    print("\n📭 Nhận tín hiệu kết thúc từ Camera Server")
                    break
                
                if '|' in line:
                    try:
                        frame_id, frame_base64 = line.split('|', 1)
                        frame_id = int(frame_id)
                        received_frames.append((frame_id, frame_base64))
                        print(f"📥 Nhận frame {frame_id}: {len(frame_base64)} bytes")
                    except Exception as e:
                        print(f"⚠️  Lỗi parse: {e}")
            
            if "END" in buffer:
                break
    
    except Exception as e:
        print(f"❌ Lỗi nhận: {e}")
    
    finally:
        conn.close()
        server_socket.close()
    
    print(f"\n📊 Tổng frame nhận: {len(received_frames)}")
    
    if not received_frames:
        print("❌ Không nhận được frame nào!")
        sc.stop()
        return
    
    # ========== XỬ LÝ FRAME BẰNG SPARK ==========
    print("\n🚀 Xử lý xóa nền bằng Spark RDD...")
    print("-" * 70)
    
    try:
        # Tạo RDD từ received frames
        num_partitions = max(1, len(received_frames) // 5)
        frames_rdd = sc.parallelize(received_frames, numSlices=num_partitions)
        
        print(f"   RDD partitions: {frames_rdd.getNumPartitions()}")
        
        # Spark transformation 1: Decode base64 thành frame
        def decode_frame(frame_tuple):
            fid, fdata = frame_tuple
            frame = decode_base64_to_frame(fdata)
            return (fid, frame) if frame is not None else None
        
        decoded_rdd = frames_rdd.map(decode_frame).filter(lambda x: x is not None)
        
        # Spark transformation 2: Xóa nền
        def remove_bg(frame_tuple):
            fid, frame = frame_tuple
            try:
                processed = remove_background(frame)
                return (fid, processed, True)
            except Exception as e:
                # Nếu lỗi, trả về frame gốc
                return (fid, frame, False)
        
        processed_rdd = decoded_rdd.map(remove_bg)
        
        # Collect kết quả
        processed_frames = processed_rdd.collect()
        
        print(f"✅ Spark xử lý xong {len(processed_frames)} frame")
        
        # ========== LƯU FRAME THÀNH FILE ẢNH ==========
        print("\n💾 Lưu frame thành file ảnh...")
        print("-" * 70)
        
        saved_count = 0
        for fid, frame, bg_removed in processed_frames:
            filename = save_frame_to_file(frame, fid, Config.OUTPUT_DIR)
            if filename:
                saved_count += 1
                status = "✅ xóa nền" if bg_removed else "⚠️ giữ nguyên"
                print(f"💾 Frame {fid}: {filename} ({status})")
        
        print(f"\n📊 Tổng file ảnh lưu: {saved_count}")
        print(f"📁 Thư mục output: {os.path.abspath(Config.OUTPUT_DIR)}")
    
    except Exception as e:
        print(f"❌ Lỗi xử lý Spark: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("\n" + "=" * 70)
        print("🔌 Dọn dẹp...")
        sc.stop()
        print("✅ Processing Server kết thúc")
        print("=" * 70)

if __name__ == "__main__":
    main()
