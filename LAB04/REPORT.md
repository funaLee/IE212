## 1. Yêu cầu
### Yêu cầu 1: Module giả lập Camera Server

| Tiêu chí | File | Mô tả |
|----------|------|-------|
| Nhận hình ảnh | `camera_server.py` | Đọc ảnh từ thư mục `input_imgs/` |
| Chuyển thành gói tin | `camera_server.py` | Encode ảnh sang Base64, đóng gói JSON với metadata (timestamp, frame_id, filename) |
| Gửi đến server xử lý | `camera_server.py` | Gửi qua TCP socket tại `localhost:9999` |
| Streaming liên tục | `camera_server.py` | Gửi frame liên tục với delay 0.5s mô phỏng camera stream |

**Cấu trúc gói tin:**
```json
{
    "timestamp": 1704545123.456,
    "frame_id": 0,
    "filename": "test_person.jpg",
    "data": "<base64_encoded_image>"
}
```

### Yêu cầu 2: Module Server Xử lý với Spark
| Tiêu chí | File | Mô tả |
|----------|------|-------|
| Nhận gói tin streaming | `spark_processor.py` | Sử dụng Spark Structured Streaming đọc từ socket |
| Xóa nền từng frame | `spark_processor.py` | Sử dụng MediaPipe Selfie Segmenter |
| Lưu thành file ảnh | `spark_processor.py` | Lưu vào `output_imgs/` với format `processed_{frame_id}_{filename}` |
| Xử lý trong ngữ cảnh Spark | `spark_processor.py` | Sử dụng `foreachBatch` + `mapPartitions` trên RDD |

**Luồng xử lý Spark:**
```
Socket Stream → Parse JSON → foreachBatch → RDD.mapPartitions → Background Removal → Save Image
```

---

## 2. Hướng dẫn chạy

### Bước 1: Cài đặt môi trường
```bash
cd /home/funalee/UIT/IE212/LAB04
python3 -m venv venv
source venv/bin/activate
pip install pyspark==3.5.3 mediapipe opencv-python
```

### Bước 2: Chuẩn bị dữ liệu
- Đặt ảnh đầu vào (`.jpg`, `.jpeg`, `.png`) vào thư mục `input_imgs/`
- Model: `models/selfie_segmenter.tflite`

### Bước 3: Chạy Camera Server (Terminal 1)
```bash
python camera_server.py
```

### Bước 4: Chạy Spark Processor (Terminal 2)
```bash
python spark_processor.py
```

### Bước 5: Dừng chương trình
Nhấn `Ctrl+C` ở cả 2 terminal khi muốn dừng.

---

## 3. Kết quả

### Input
| File | Kích thước |
|------|------------|
| `test_person.jpg` | 97 KB |
| `test_person_2.jpg` | 512 KB |

- Thêm 2 ảnh trong thư mục `input_imgs/`, giả sử làm ảnh được chụp bởi camera, dùng vòng lặp while true để giả sử camera chụp liên tục.

### Output
| File mẫu | Kích thước | Mô tả |
|----------|------------|-------|
| `processed_0_test_person.jpg` | 11 KB | Ảnh đã xóa nền, nền thay bằng màu xám |
| `processed_1_test_person_2.jpg` | 631 KB | Ảnh đã xóa nền, nền thay bằng màu xám |
| ... | ... | Tổng cộng 45 frame đã xử lý |

- Giả sử chạy được 45 frame từ 2 ảnh đầu vào, lưu vào thư mục `output_imgs/`

### Log Camera Server
```
Found 2 images.
Camera Server listening on localhost:9999
Connected by ('127.0.0.1', 58578)
Sent frame 0 (test_person.jpg)
Sent frame 1 (test_person_2.jpg)
...
```

### Log Spark Processor
```
Batch 0 processed 2 frames.
(0, 'test_person.jpg', 'Success', 'processed_0_test_person.jpg')
(1, 'test_person_2.jpg', 'Success', 'processed_1_test_person_2.jpg')
...
```