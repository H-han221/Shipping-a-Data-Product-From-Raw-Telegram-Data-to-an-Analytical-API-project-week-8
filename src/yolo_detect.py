import os
import pandas as pd
from ultralytics import YOLO

IMAGE_ROOT = "data/raw/images"
OUTPUT_CSV = "data/processed/yolo_detections.csv"

model = YOLO("yolov8n.pt")

records = []

for channel in os.listdir(IMAGE_ROOT):
    channel_path = os.path.join(IMAGE_ROOT, channel)
    if not os.path.isdir(channel_path):
        continue

    for img_file in os.listdir(channel_path):
        if not img_file.endswith(".jpg"):
            continue

        message_id = img_file.replace(".jpg", "")
        img_path = os.path.join(channel_path, img_file)

        results = model(img_path, verbose=False)

        detected = []
        for r in results:
            for box in r.boxes:
                cls = int(box.cls[0])
                label = model.names[cls]
                conf = float(box.conf[0])
                detected.append((label, conf))

        labels = [d[0] for d in detected]

        # Classification rules
        if "person" in labels and any(x in labels for x in ["bottle", "cup"]):
            category = "promotional"
        elif any(x in labels for x in ["bottle", "cup"]):
            category = "product_display"
        elif "person" in labels:
            category = "lifestyle"
        else:
            category = "other"

        for label, conf in detected:
            records.append({
                "message_id": message_id,
                "channel_name": channel,
                "detected_object": label,
                "confidence_score": conf,
                "image_category": category
            })

df = pd.DataFrame(records)
df.to_csv(OUTPUT_CSV, index=False)

print(f"Saved YOLO results to {OUTPUT_CSV}")
