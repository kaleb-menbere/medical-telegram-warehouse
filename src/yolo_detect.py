import os
import csv
import glob
from pathlib import Path
from ultralytics import YOLO
import cv2

def detect_objects_in_images():
    """
    Run YOLOv8 object detection on downloaded Telegram images
    and categorize them based on detected objects.
    """
    
    # Initialize YOLO model
    print("🚀 Loading YOLOv8 model...")
    model = YOLO('yolov8n.pt')  # Using nano model for efficiency
    
    # Define detection categories
    CATEGORIES = {
        'promotional': {'person', 'bottle', 'container'},
        'product_display': {'bottle', 'container', 'packet', 'box'},
        'lifestyle': {'person'},
        'other': set()
    }
    
    # Prepare output CSV
    output_file = "data/yolo_detections.csv"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Find all downloaded images
    image_dir = "data/raw/images"
    image_paths = []
    
    for ext in ['*.jpg', '*.jpeg', '*.png', '*.JPG', '*.JPEG']:
        image_paths.extend(glob.glob(os.path.join(image_dir, '**', ext), recursive=True))
    
    print(f"📸 Found {len(image_paths)} images to process")
    
    # Process each image
    results = []
    
    for img_path in image_paths:
        try:
            # Extract message_id from filename (format: messageid_timestamp.jpg)
            filename = os.path.basename(img_path)
            message_id = filename.split('_')[0]
            
            # Get channel name from directory structure
            channel_name = Path(img_path).parent.name
            
            # Run YOLO detection
            detection_results = model(img_path, verbose=False)[0]
            
            # Extract detected objects
            detected_objects = set()
            for box in detection_results.boxes:
                class_id = int(box.cls[0])
                class_name = detection_results.names[class_id]
                confidence = float(box.conf[0])
                
                if confidence > 0.5:  # Only consider confident detections
                    detected_objects.add(class_name)
            
            # Categorize image based on detected objects
            image_category = 'other'
            if 'person' in detected_objects and any(obj in detected_objects for obj in ['bottle', 'container', 'packet', 'box']):
                image_category = 'promotional'
            elif any(obj in detected_objects for obj in ['bottle', 'container', 'packet', 'box']):
                image_category = 'product_display'
            elif 'person' in detected_objects:
                image_category = 'lifestyle'
            
            # Store results
            results.append({
                'message_id': message_id,
                'channel_name': channel_name,
                'image_path': img_path,
                'detected_objects': ', '.join(sorted(detected_objects)),
                'num_detections': len(detected_objects),
                'image_category': image_category,
                'confidence_score': 0.8 if detected_objects else 0.1  # Simplified confidence
            })
            
            print(f"  ✓ Processed: {channel_name}/{filename} -> {image_category} ({len(detected_objects)} objects)")
            
        except Exception as e:
            print(f"  ✗ Error processing {img_path}: {e}")
    
    # Save results to CSV
    if results:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['message_id', 'channel_name', 'image_path', 'detected_objects', 
                         'num_detections', 'image_category', 'confidence_score']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\n✅ Saved {len(results)} detections to {output_file}")
        
        # Print summary
        categories_count = {}
        for result in results:
            cat = result['image_category']
            categories_count[cat] = categories_count.get(cat, 0) + 1
        
        print("\n📊 Detection Summary:")
        for category, count in categories_count.items():
            print(f"  {category}: {count} images")
    
    else:
        print("⚠️ No images were processed successfully")

if __name__ == "__main__":
    detect_objects_in_images()