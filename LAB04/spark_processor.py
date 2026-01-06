from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
import cv2
import base64
import numpy as np
import mediapipe as mp
from mediapipe.tasks import python
from mediapipe.tasks.python import vision
import os
import time
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Configurations
MODEL_PATH = "models/selfie_segmenter.tflite"
OUTPUT_DIR = "output_imgs"
BG_COLOR = (192, 192, 192) # gray
MASK_COLOR = (255, 255, 255) # white

def init_segmenter():
    """Initializes the MediaPipe Image Segmenter."""
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(f"Model not found at {MODEL_PATH}")

    base_options = python.BaseOptions(model_asset_path=MODEL_PATH)
    options = vision.ImageSegmenterOptions(base_options=base_options, output_category_mask=True)
    return vision.ImageSegmenter.create_from_options(options)

def process_partition(iterator):
    """
    Process a partition of records.
    Initializes the segmenter once per partition.
    """
    segmenter = init_segmenter()
    
    for row in iterator:
        img_data_b64 = row.data
        frame_id = row.frame_id
        timestamp = row.timestamp
        filename = row.filename
        
        try:
            # Decode image
            img_bytes = base64.b64decode(img_data_b64)
            np_arr = np.frombuffer(img_bytes, np.uint8)
            frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
            
            if frame is None:
                continue
                
            # Remove background
            mp_image = mp.Image(image_format=mp.ImageFormat.SRGB, data=frame)
            segmentation_result = segmenter.segment(mp_image)
            category_mask = segmentation_result.category_mask

            # remove the background
            image_data = mp_image.numpy_view()
            bg_image = np.zeros(image_data.shape, dtype=np.uint8)
            bg_image[:] = BG_COLOR
            
            mask = category_mask.numpy_view()
            # Ensure mask is 2D (H, W)
            if mask.ndim == 3:
                mask = mask.squeeze()

            condition = np.stack((mask,) * 3, axis=-1) > 0.1
            output_frame = np.where(condition, image_data, bg_image)

            # Save to disk
            out_name = f"processed_{frame_id}_{filename}"
            cv2.imwrite(os.path.join(OUTPUT_DIR, out_name), output_frame)
            
            yield (frame_id, filename, "Success", out_name)
            
        except Exception as e:
            yield (frame_id, filename, f"Error: {str(e)}", "")


def run_spark_job():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    spark = SparkSession.builder \
        .appName("BackgroundRemover") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("timestamp", DoubleType(), True),
        StructField("frame_id", LongType(), True),
        StructField("filename", StringType(), True),
        StructField("data", StringType(), True)
    ])

    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    df = lines.select(from_json(col("value"), schema).alias("json_data")).select("json_data.*")

    def process_batch_func(batch_df, batch_id):
        if batch_df.count() > 0:
            results = batch_df.rdd.mapPartitions(process_partition).collect()
            print(f"Batch {batch_id} processed {len(results)} frames.")
            for res in results:
                print(res)
    
    query = df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch_func) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_spark_job()
