import socket
import cv2
import json
import base64
import time
import os
import glob
import sys

HOST = 'localhost'
PORT = 9999


def get_images():
    img_patterns = ['*.jpg', '*.jpeg', '*.png']
    files = []
    for pattern in img_patterns:
        files.extend(glob.glob(os.path.join('input_imgs', pattern)))
    return sorted(files)

def image_to_base64(path):
    img = cv2.imread(path)
    if img is None:
        return None
    _, buffer = cv2.imencode('.jpg', img)
    return base64.b64encode(buffer).decode('utf-8')

def run_server():
    images = get_images()
    if not images:
        print("No images found in input_imgs/")
        return

    print(f"Found {len(images)} images.")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        print(f"Camera Server listening on {HOST}:{PORT}")
        
        while True:
            try:
                conn, addr = s.accept()
                with conn:
                    print(f"Connected by {addr}")
                    
                    frame_id = 0
                    try:
                        while True:
                            for img_path in images:
                                b64_data = image_to_base64(img_path)
                                if b64_data:
                                    payload = {
                                        "timestamp": time.time(),
                                        "frame_id": frame_id,
                                        "filename": os.path.basename(img_path),
                                        "data": b64_data
                                    }
                                    # Send as JSON line
                                    message = json.dumps(payload) + "\n"
                                    conn.sendall(message.encode('utf-8'))
                                    print(f"Sent frame {frame_id} ({os.path.basename(img_path)})")
                                    frame_id += 1
                                    time.sleep(0.5) # Simulate delay
                    except (BrokenPipeError, ConnectionResetError):
                        print("Client disconnected.")
                    except Exception as e:
                        print(f"Error during transmission: {e}")
            except Exception as e:
                print(f"Accept error: {e}")
                time.sleep(1)

if __name__ == "__main__":
    run_server()
