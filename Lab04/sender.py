"""
MODULE 1: CAMERA SERVER (SENDER)
================================
- Giả lập camera nhận hình ảnh
- Chuyển hình ảnh thành các gói tin  
- Gửi đến server xử lý qua TCP
- Sử dụng Spark để xử lý frame
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import socket
import struct
import cv2
import numpy as np
import time
import base64

class Config:
    """Cấu hình"""
    RECEIVER_HOST = "localhost"
    RECEIVER_PORT = 6100
    SPARK_APP_NAME = "CameraServer_Spark"
    BATCH_DURATION = 1
    FRAME_DELAY = 100  # milliseconds giữa các frame

def encode_frame_to_base64(frame):
    """Encode frame thành base64 string để truyền qua TCP"""
    try:
        ret, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
        if ret:
            return base64.b64encode(buffer).decode('utf-8')
        return None
    except Exception as e:
        print(f"❌ Lỗi encode: {e}")
        return None

def send_frame_packet(sock, frame_id, frame_base64):
    """Gửi gói tin frame qua TCP socket"""
    try:
        # Format: frame_id|base64_data\n
        message = f"{frame_id}|{frame_base64}\n"
        sock.sendall(message.encode('utf-8'))
        return True
    except Exception as e:
        print(f"❌ Lỗi gửi: {e}")
        return False

def main():
    print("=" * 70)
    print("📹 MODULE 1: CAMERA SERVER (Spark)")
    print("   Giả lập camera → Chuyển frame thành gói tin → Gửi TCP")
    print("=" * 70)
    
    # ========== KHỞI TẠO SPARK ==========
    sc = SparkContext(appName=Config.SPARK_APP_NAME, master="local[*]")
    sc.setLogLevel("ERROR")
    
    print(f"\n✅ Spark Context khởi tạo: {Config.SPARK_APP_NAME}")
    print(f"   Parallelism: {sc.defaultParallelism}")
    
    # Khởi tạo Spark Streaming
    ssc = StreamingContext(sc, Config.BATCH_DURATION)
    print(f"✅ Spark Streaming khởi tạo (batch: {Config.BATCH_DURATION}s)")
    
    # ========== MỞ NGUỒN VIDEO/CAMERA ==========
    print("\n📷 Mở nguồn video...")
    
    # Ưu tiên mở camera trước
    cap = cv2.VideoCapture(0)
    
    if not cap.isOpened():
        print("⚠️  Không thể mở camera, thử dùng test_video.mp4...")
        cap = cv2.VideoCapture('test_video.mp4')
    
    if not cap.isOpened():
        print("❌ Không thể mở camera hoặc video!")
        sc.stop()
        return
    
    # Kiểm tra nguồn đang dùng
    if cap.get(cv2.CAP_PROP_FRAME_COUNT) > 0:
        print("📹 Đang dùng: VIDEO FILE")
    else:
        print("📹 Đang dùng: CAMERA")
    
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    print(f"✅ Video: {width}x{height}, tổng {total_frames} frames")
    
    # ========== KẾT NỐI TỚI RECEIVER ==========
    print(f"\n🔗 Kết nối tới Receiver ({Config.RECEIVER_HOST}:{Config.RECEIVER_PORT})...")
    
    sock = None
    for attempt in range(10):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((Config.RECEIVER_HOST, Config.RECEIVER_PORT))
            print("✅ Kết nối TCP thành công!")
            break
        except Exception as e:
            print(f"⚠️  Lần {attempt+1}/10: Chờ Receiver khởi động... ({e})")
            if sock:
                sock.close()
            sock = None
            time.sleep(2)
    
    if sock is None:
        print("❌ Không thể kết nối tới Receiver! Hãy chạy receiver.py trước.")
        cap.release()
        sc.stop()
        return
    
    # ========== ĐỌC FRAME TỪ VIDEO ==========
    print("\n📖 Đọc frame từ video...")
    
    frames_data = []
    frame_id = 0
    
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        
        # Resize frame để giảm kích thước
        frame = cv2.resize(frame, (640, 480))
        
        # Encode frame thành base64
        frame_base64 = encode_frame_to_base64(frame)
        if frame_base64:
            frames_data.append((frame_id, frame_base64))
            frame_id += 1
    
    cap.release()
    print(f"✅ Đọc xong {len(frames_data)} frame")
    
    # ========== XỬ LÝ FRAME BẰNG SPARK RDD ==========
    print("\n🚀 Xử lý frame bằng Spark RDD...")
    print("-" * 70)
    
    try:
        # Tạo RDD từ danh sách frame
        num_partitions = max(1, len(frames_data) // 10)
        frames_rdd = sc.parallelize(frames_data, numSlices=num_partitions)
        
        print(f"   RDD partitions: {frames_rdd.getNumPartitions()}")
        
        # Spark transformation: thêm metadata (timestamp, size)
        def add_metadata(frame_tuple):
            fid, fdata = frame_tuple
            metadata = {
                'frame_id': fid,
                'size': len(fdata),
                'timestamp': time.time()
            }
            return (fid, fdata, metadata)
        
        # Apply transformation
        processed_rdd = frames_rdd.map(add_metadata)
        
        # Collect kết quả từ Spark
        processed_frames = processed_rdd.collect()
        
        print(f"✅ Spark xử lý xong {len(processed_frames)} frame")
        
        # ========== GỬI FRAME QUA TCP ==========
        print("\n📡 Gửi gói tin frame qua TCP...")
        print("-" * 70)
        
        sent_count = 0
        for fid, fdata, metadata in processed_frames:
            if send_frame_packet(sock, fid, fdata):
                sent_count += 1
                print(f"✅ Gửi frame {fid}: {metadata['size']} bytes")
            else:
                print(f"❌ Lỗi gửi frame {fid}")
                break
            
            # Delay giữa các frame
            time.sleep(Config.FRAME_DELAY / 1000.0)
        
        # Gửi tín hiệu kết thúc
        sock.sendall(b"END\n")
        print(f"\n📊 Tổng frame gửi thành công: {sent_count}/{len(processed_frames)}")
    
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("\n" + "=" * 70)
        print("🔌 Dọn dẹp...")
        sock.close()
        sc.stop()
        print("✅ Camera Server kết thúc")
        print("=" * 70)

if __name__ == "__main__":
    main()
