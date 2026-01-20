# **Task 3: Data Enrichment with Object Detection (YOLO)**

## **✅ STATUS: COMPLETED SUCCESSFULLY**

## **📋 Overview**
Implemented a computer vision pipeline using YOLOv8 to analyze Telegram medical channel images, categorize them, and integrate insights into the data warehouse for analytical queries.

## **🎯 Objectives Achieved**
1. ✓ Set up YOLO environment and detect objects in Telegram images
2. ✓ Categorize images based on detected objects
3. ✓ Integrate detection results into data warehouse
4. ✓ Analyze patterns in visual content usage
5. ✓ Answer business questions about image engagement

## **🛠 Technical Implementation**

### **1. YOLO Environment Setup**
- Installed `ultralytics` library for YOLOv8
- Used YOLOv8n (nano model) for efficiency on standard hardware
- Configured confidence threshold: 0.5

### **2. Object Detection Script (`src/yolo_detect.py`)**
- Processed 162 Telegram images from medical channels
- Implemented 4-category classification system:
  - **promotional**: Person + product (someone showing/holding item)
  - **product_display**: Bottle/container, no person
  - **lifestyle**: Person, no product
  - **other**: Neither detected
- Saved results to `data/yolo_detections.csv`

### **3. Database Integration**
- Created PostgreSQL table: `raw.yolo_detections`
- Loaded 162 detection records
- Schema includes: message_id, channel_name, image_path, detected_objects, image_category, confidence_score

### **4. dbt Data Model (`fct_image_detections`)**
- Created fact table joining YOLO results with messages and channels
- 130 successful joins (80.2% match rate)
- Integrated into star schema with foreign keys to dim_channels and dim_dates

### **5. Analysis Pipeline**
- Created `scripts/analyze_yolo_results.py` for business insights
- Automated data quality checks and reporting

## **📊 Key Results & Insights**

### **Image Category Distribution:**
| Category | Images | Percentage | Avg Confidence |
|----------|--------|------------|----------------|
| Other | 98 | 60.5% | 0.19 |
| Lifestyle | 32 | 19.8% | 0.80 |
| Product Display | 32 | 19.8% | 0.80 |
| Promotional | 0 | 0% | N/A |

### **Engagement Analysis (Views per Category):**
| Category | Posts | Avg Views | Avg Forwards |
|----------|-------|-----------|--------------|
| Lifestyle | 30 | 5,747 | 4.5 |
| Other | 74 | 359 | 0.5 |
| Product Display | 26 | 339 | 0.5 |

**🚀 KEY INSIGHT: Lifestyle posts get 16x more views than product displays!**

### **Channel Visual Content Usage:**
| Channel | Total Posts | Posts with Images | Image % | Categories Detected |
|---------|-------------|-------------------|---------|-------------------|
| Lobelia pharmacy and cosmetics | 44 | 44 | 100% | lifestyle, other, product_display |
| Tikvah | Pharma | 37 | 21 | 56.8% | lifestyle, other, product_display |

**📸 KEY INSIGHT: Lobelia uses images in 100% of posts vs 56.8% for Tikvah**

### **Most Common Detected Objects:**
| Object | Detections | Percentage |
|--------|------------|------------|
| None detected | 86 | 46.2% |
| Bottle | 32 | 17.2% |
| Person | 32 | 17.2% |
| Refrigerator | 6 | 3.2% |
| Orange | 4 | 2.2% |

## **🔧 Files Created**

### **Scripts:**
- `src/yolo_detect.py` - Main YOLO detection script
- `scripts/load_yolo_results.py` - Database loader
- `scripts/analyze_yolo_results.py` - Business analysis
- `scripts/verify_task3_complete.py` - Verification script

### **Data Models:**
- `medical_warehouse/models/marts/fct_image_detections.sql` - Fact table
- Updated `medical_warehouse/models/sources.yml` - Added YOLO source

### **Data Outputs:**
- `data/yolo_detections.csv` - Raw detection results (162 records)
- PostgreSQL table: `raw.yolo_detections`
- Data mart: `medical_warehouse_marts.fct_image_detections` (130 records)

## **🎯 Business Questions Answered**

### **1. Do "promotional" posts get more views?**
- **Answer**: No promotional posts detected (0%)
- **Insight**: Medical channels don't use people+product promotional imagery

### **2. Which channels have the most visual content?**
- **Answer**: Lobelia pharmacy and cosmetics (100% vs Tikvah's 56.8%)
- **Insight**: Lobelia is highly visual, Tikvah mixes text and images

### **3. What gets more engagement?**
- **Answer**: Lifestyle images (5,747 avg views vs 339 for product displays)
- **Insight**: Posts with people get 16x more views than product-only posts

## **⚠️ Limitations & Challenges**

### **Technical Challenges Solved:**
1. **PostgreSQL ROUND function**: Fixed by casting to numeric
2. **dbt source definition**: Properly configured in sources.yml
3. **Data joining**: 80.2% successful match rate achieved

### **Model Limitations:**
1. **Domain specificity**: Pre-trained YOLO detects general objects, not medical products
2. **High "other" category**: 60.5% of images unclassifiable
3. **No medical product recognition**: Cannot identify specific drugs or medical equipment
4. **Confidence variability**: Depends on image quality and object clarity

### **Detection Accuracy:**
- **46.2%**: No objects detected (images too specific or poor quality)
- **36.4%**: Successful detections (person, bottle, etc.)
- **17.4%**: Miscellaneous objects (refrigerator, orange, clock, etc.)

## **✅ Quality Assurance**

### **dbt Tests:**
- All 27 data tests passing
- Referential integrity validated
- Data quality checks implemented

### **Data Validation:**
- ✅ 162 images processed
- ✅ 130 successful data joins
- ✅ All categories correctly calculated
- ✅ Confidence scores within expected range

### **Pipeline Reliability:**
- End-to-end automation achieved
- Error handling implemented
- Logging and monitoring in place

## **📈 Performance Metrics**

| Metric | Value |
|--------|-------|
| Images processed | 162 |
| Successful joins | 130 (80.2%) |
| Processing time | < 5 minutes |
| Detection confidence (avg) | 0.59 |
| Categories identified | 3/4 |

## **🔮 Recommendations & Next Steps**

### **Short-term:**
1. **Combine with text analysis**: Merge image and text insights
2. **API exposure**: Include YOLO insights in analytical API (Task 4)
3. **Dashboard creation**: Visualize image engagement patterns

### **Long-term:**
1. **Fine-tune YOLO**: Train on medical product images
2. **Custom categories**: Create domain-specific classification
3. **Multi-modal analysis**: Combine image, text, and metadata

## **🏗 Architecture Diagram**

```
Telegram Images (162)
        ↓
YOLOv8 Object Detection
        ↓
CSV Results (detected_objects, category, confidence)
        ↓
PostgreSQL Load (raw.yolo_detections)
        ↓
dbt Transformation (fct_image_detections)
        ↓
Star Schema Integration
        ↓
Analytical Queries
        ↓
Business Insights
```

## **📝 Conclusion**

Task 3 successfully demonstrated how computer vision can enrich a data warehouse with image-based insights. While the pre-trained YOLO model has limitations for domain-specific medical content, it still provided valuable insights about engagement patterns and channel strategies.

**Key success**: Lifestyle images drive significantly higher engagement (16x more views), providing actionable insights for content strategy.

**Ready for**: Task 4 - Building an Analytical API to expose these insights to business users.

---

**Completion Date**: January 20, 2026  
**Next Task**: Task 4 - Build an Analytical API with FastAPI