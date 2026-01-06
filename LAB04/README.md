# Lab 04: Spark Background Removal

## Prerequisites
- Python 3.8+
- Java 8, 11, or 17 (PySpark 3.5.3 compatible)
- Pip

## Installation
1. Create a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```
2. Install dependencies:
   ```bash
   pip install pyspark==3.5.3 mediapipe opencv-python
   ```

## Usage

1. **Start Camera Server**:
   ```bash
   python camera_server.py
   ```
   This simulates a camera stream on `localhost:9999`.

2. **Start Spark Processor**:
   ```bash
   python spark_processor.py
   ```

## Results
- Processed images (background removed) will be saved in `output_imgs/`.

## Notes
- The model `input_imgs/selfie_segmenter.tflite` is required (handled by script).
- Ensure no other process is using port 9999.
