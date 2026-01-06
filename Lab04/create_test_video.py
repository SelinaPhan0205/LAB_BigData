"""
Tạo video test với hình người đơn giản để test background removal
"""
import cv2
import numpy as np

print("📹 Tạo test video file...")

output_file = "test_video.mp4"
width, height = 640, 480
fps = 30
duration = 5  # 5 giây
total_frames = fps * duration

# Khởi tạo video writer
fourcc = cv2.VideoWriter_fourcc(*'mp4v')
out = cv2.VideoWriter(output_file, fourcc, fps, (width, height))

# Tạo frame
for i in range(total_frames):
    # Tạo background gradient
    frame = np.zeros((height, width, 3), dtype=np.uint8)
    
    for y in range(height):
        color_val = int((y / height) * 200)
        frame[y, :] = [color_val, 100, max(0, 200 - color_val)]
    
    # Vẽ hình người (oval head + body)
    center_x = width // 2 + int(50 * np.sin(i * 0.05))
    
    # Head (oval)
    head_center = (center_x, 150)
    cv2.ellipse(frame, head_center, (40, 50), 0, 0, 360, (200, 180, 160), -1)
    
    # Body (rectangle)
    body_top = 200
    body_bottom = 400
    cv2.rectangle(frame, (center_x - 60, body_top), (center_x + 60, body_bottom), (100, 100, 200), -1)
    
    # Arms
    cv2.line(frame, (center_x - 60, 220), (center_x - 100, 300), (200, 180, 160), 15)
    cv2.line(frame, (center_x + 60, 220), (center_x + 100, 300), (200, 180, 160), 15)
    
    # Text
    text = f"Frame {i+1}/{total_frames}"
    cv2.putText(frame, text, (20, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2)
    
    out.write(frame)
    
    if (i + 1) % 30 == 0:
        print(f"  ✓ Created {i+1}/{total_frames} frames")

out.release()
print(f"✅ Video test created: {output_file} ({width}x{height} @ {fps} FPS, {total_frames} frames)")
