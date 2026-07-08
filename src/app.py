import os
import json
import logging
import uuid
from datetime import datetime
from fastapi import FastAPI, Request, Response, status
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI()

# Bật CORS cho phép gọi API từ giao diện HTML
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "recruitment-tracking")

# Khởi tạo Kafka Producer kết nối tới Kafka Broker (Lazy Loading)
producer = None

def get_producer():
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                max_block_ms=2000,          # Không block API quá 2 giây nếu Kafka chưa sẵn sàng
                request_timeout_ms=2000      # Timeout request sau 2 giây
            )
            logging.info(f"Kết nối Kafka thành công tới {KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            logging.error(f"Khởi tạo Kafka Producer thất bại (sẽ tự động kết nối lại ở request tiếp theo): {str(e)}")
    return producer

# Thử kết nối lần đầu khi chạy ứng dụng (nếu lỗi sẽ retry lazily khi nhận request)
get_producer()

# Endpoint trả về giao diện HTML Dashboard tương tác
@app.get("/api/ui", response_class=HTMLResponse)
async def get_ui():
    try:
        with open("/app/index.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        return html_content
    except Exception as e:
        logging.error(f"Lỗi đọc index.html: {str(e)}")
        return HTMLResponse(content=f"Failed to load UI: {str(e)}", status_code=500)

# Endpoint phục vụ ảnh screenshot của Grafana Dashboard tĩnh
@app.get("/api/doc/grafana_dashboard.png")
async def get_dashboard_image():
    image_path = "/app/doc/grafana_dashboard.png"
    if os.path.exists(image_path):
        return FileResponse(image_path, media_type="image/png")
    return Response(content="Image not found", status_code=404)

# Endpoint tiếp nhận sự kiện tracking (POST /api/track)
@app.post("/api/track")
async def track_event(request: Request, response: Response):
    try:
        body = await request.json()
        
        bid = int(float(body.get("bid", 0.0)))
        campaign_id = int(body.get("campaign_id", 0))
        custom_track = body.get("custom_track", "click")
        group_id = body.get("group_id")
        if group_id is not None:
            group_id = int(group_id)
            
        job_id = int(body.get("job_id", 0))
        publisher_id = int(body.get("publisher_id", 0))
        
        # Tạo mã định danh create_time dạng UUIDv1 và timestamp
        create_time = str(uuid.uuid1())
        ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        
        # Đóng gói payload
        payload = {
            "create_time": create_time,
            "bid": bid,
            "campaign_id": campaign_id,
            "custom_track": custom_track,
            "group_id": group_id,
            "job_id": job_id,
            "publisher_id": publisher_id,
            "ts": ts
        }
        
        active_producer = get_producer()
        if active_producer:
            # Gửi bất đồng bộ vào Kafka (không block luồng API)
            active_producer.send(KAFKA_TOPIC, payload)
            logging.info(f"Đã gửi sự kiện vào Kafka: {create_time}")
            return {
                "status": "accepted",
                "message": "Event recorded and sent to streaming pipeline.",
                "event": {
                    "create_time": create_time,
                    "ts": ts,
                    "custom_track": custom_track,
                    "job_id": job_id
                }
            }
        else:
            logging.error("Kafka producer chưa sẵn sàng.")
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return {"status": "error", "message": "Streaming broker unavailable"}
            
    except Exception as e:
        logging.error(f"Ingestion API thất bại: {str(e)}")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"status": "error", "message": f"Ingestion failed: {str(e)}"}