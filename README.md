# Recruitment Data Pipeline - Real-time Clickstream Processing

Hệ thống Data Pipeline xử lý hành vi người dùng thời gian thực (Real-time Clickstream Processing) cho nền tảng tuyển dụng trực tuyến, chuyển đổi kiến trúc từ Batch Processing cũ sang **Event-Driven Streaming Architecture** sử dụng **FastAPI, Apache Kafka, Apache Cassandra, PySpark Structured Streaming, MySQL và Grafana**.

---

## 1. Tổng Quan Dự Án

Dự án xây dựng hạ tầng Data Pipeline thời gian thực nhằm xử lý luồng dữ liệu tương tác ứng viên (Clickstream) trên hệ thống tuyển dụng. Hệ thống hỗ trợ đo lường hiệu suất chạy quảng cáo đẩy tin tuyển dụng, tự động tính toán chi phí (Spend) và các chỉ số KPI (Clicks, Conversions, Qualified CVs) theo thời gian thực với độ trễ xử lý ~7 giây.

---

## 2. Khái Niệm Nghiệp Vụ (Programmatic Job Ads)

Nền tảng tuyển dụng hoạt động theo mô hình chạy quảng cáo bài đăng tuyển dụng:
* **Quảng cáo tin tuyển dụng:** Trang tuyển dụng mua quảng cáo trên Facebook/Google (`Publisher`) để tìm ứng viên cho bài đăng.
* **Chi phí quảng cáo (Spend):** Trả tiền cho Publisher trên mỗi click nhấp xem tin (`Spend = Clicks × Bid`).
* **Clickstream:** Luồng dữ liệu ghi lại hành vi ứng viên (`click` xem tin, `conversion` nộp CV, `qualified` CV đạt chuẩn).
* **KPI Tuyển dụng:** Số liệu tổng hợp gồm Clicks, Conversions, Qualified CVs và Tổng chi phí Spend.

---

## 3. Bài Toán Nghiệp Vụ & Giá Trị Giải Pháp

### Bài toán khi thiếu Pipeline thời gian thực:
* **Thiếu thông tin đo lường:** Phân bổ ngân sách chạy quảng cáo tin tuyển dụng theo cảm tính do thiếu số liệu báo cáo tức thời.
* **Lãng phí chi phí quảng cáo:** Tiếp tục đốt tiền quảng cáo cho các nguồn không mang lại CV thực tế, hoặc tiếp tục chạy quảng cáo cho tin đã tuyển đủ.
* **Trễ hạn cam kết tuyển dụng:** Không phát hiện kịp thời các tin tuyển dụng gấp đang thiếu CV để điều chỉnh ngân sách.
* **Tải nặng lên database ứng dụng:** Chạy truy vấn báo cáo trực tiếp làm nghẽn cơ sở dữ liệu vận hành chính.

### Giá trị sau khi triển khai Pipeline:
* **Minh bạch số liệu 100%:** Theo dõi tức thời hiệu suất từng tin tuyển dụng, từng chiến dịch và kênh quảng cáo trên Grafana Dashboard.
* **Độ trễ xử lý ~7 giây:** Tiếp nhận qua FastAPI (2ms) ➔ Kafka ➔ PySpark xử lý stream và cập nhật số liệu sau ~7 giây.
* **Tối ưu ngân sách:** Ngắt ngay quảng cáo khi đã thu đủ CV đạt chuẩn và dồn ngân sách cho các tin tuyển dụng gấp.
* **Hạ tầng độc lập:** Raw log lưu trữ ở Cassandra, KPI lưu trữ ở MySQL, không ảnh hưởng tới database ứng dụng chính.

---

## 4. Kiến Trúc Hệ Thống & Luồng Dữ Liệu

### Sơ đồ kiến trúc (Architecture Flowchart)
```mermaid
flowchart TD
    subgraph Web_Client [Website Tuyển Dụng]
        UserInteract["Hành vi người dùng: Click, Apply, Qualify..."]
    end

    subgraph Ingestion_Layer [Tầng Tiếp Nhận]
        FastAPI["FastAPI Ingestion API (app.py)"]
        KafkaBroker[("Apache Kafka (KRaft Mode)")]
        UserInteract -->|HTTP POST /api/track| FastAPI
        FastAPI -->|Publish Events| KafkaBroker
    end

    subgraph Processing_Layer [Tầng Xử Xý Luồng]
        SparkStreaming[["PySpark Structured Streaming (streaming_pipeline.py)"]]
        KafkaBroker -->|Subscribe Stream| SparkStreaming
    end

    subgraph Storage_Layer [Tầng Lưu Trữ]
        Cassandra[("Apache Cassandra (Data Lake)")]
        MySQL[("MySQL (Data Warehouse)")]
        SparkStreaming -->|1. Lưu Log Thô| Cassandra
        SparkStreaming -->|2. Tra Cứu Job Metadata| MySQL
        SparkStreaming -->|3. Nạp Chỉ Số Tổng Hợp| MySQL
    end

    subgraph Visualization_Layer [Trực Quan Hóa]
        Grafana["Grafana Dashboard"]
        MySQL -->|Truy Vấn KPI Real-time| Grafana
    end
```

### Chi tiết các bước xử lý:
1. **API Ingestion (FastAPI):** Tiếp nhận gói tin tracking từ client, đóng gói payload, sinh định danh UUIDv1 và đẩy bất đồng bộ (Asynchronous) vào Kafka Topic `recruitment-tracking` trong vòng 2ms.
2. **Event Broker (Kafka KRaft):** Đóng vai trò hàng đợi sự kiện, lưu trữ tạm thời và đảm bảo thứ tự thời gian của luồng sự kiện.
3. **Real-time Processing (PySpark Structured Streaming):**
   * Đăng ký lắng nghe Kafka Stream, tự động phân giải JSON từ byte nhị phân dựa trên Schema định sẵn.
   * **Write to Data Lake:** Ghi trực tiếp log thô vào **Cassandra** phục vụ phân tích chuyên sâu.
   * **Stream-to-Static Join:** Ghép nối dữ liệu stream với bảng dữ liệu tĩnh `job` trong **MySQL** để lấy mã công ty (`company_id`).
   * **Aggregation:** Tổng hợp chỉ số (Clicks, Conversions, Qualified/Unqualified, Spend) theo giờ và theo ngày.
   * **Write to Data Warehouse:** Ghi đè cập nhật số liệu trực tiếp vào bảng `events` của **MySQL**.
4. **Dashboard (Grafana):** Kết nối trực tiếp vào MySQL hiển thị đồ thị tương tác thời gian thực với tổng độ trễ luồng ~7 giây.

---

## 5. Kết Quả Kiểm Thử Chịu Tải (Load Testing Benchmark)

Khả năng chịu tải của tầng Ingestion API (`POST /api/track`) được kiểm thử bằng script chuyên dụng (`src/benchmark_load_test.py`):

* **Engine:** Python `asyncio` + `aiohttp` concurrent worker pool.
* **Throughput (Xử lý thực tế):** ~156 Clicks/sec (RPS) trên 1 node server cloud.
* **Tỷ lệ thành công:** 99.46% (3,528/3,547 request thành công dưới tải 100 concurrent workers).
* **Độ trễ phản hồi (Latency):**
  * P50 (Median): 436 ms
  * P95: 1,274 ms

---

## 6. Hướng Dẫn Cài Đặt Và Vận Hành

### Yêu cầu hệ thống:
* Docker & Docker Compose.
* Python 3.10+ trên máy host.

### Bước 1: Cài đặt phụ thuộc Python
```bash
pip install pandas mysql-connector-python kafka-python requests aiohttp
```

### Bước 2: Khởi chạy hạ tầng Docker
```bash
docker compose up -d --build
```
Kiểm tra trạng thái các container:
```bash
docker compose ps
```

### Bước 3: Khởi tạo cơ sở dữ liệu
```bash
python src/init_db.py
```

### Bước 4: Chạy script giả lập dữ liệu hoặc benchmark
* **Bắn dữ liệu liên tục qua API:**
  ```bash
  python src/generate_dummy_data_api.py
  ```
* **Chạy Benchmark chịu tải (Clicks/sec):**
  ```bash
  python src/benchmark_load_test.py --url http://127.0.0.1:8082/api/track --concurrency 50 --duration 15
  ```

### Bước 5: Theo dõi log xử lý của Spark Worker
```bash
docker logs -f etl_streaming_worker
```

---

## 7. Môi Trường Cloud & API Endpoints

### Grafana Live Dashboard:
* **URL Trực Quan:** [Grafana Live Dashboard](https://loyallagoon578.grafana.net/public-dashboards/c0be1c061fef47eaa2dc37a4db5ce42a)

### API Endpoints:
* **Tracking API:** `POST http://159.223.41.98:8082/api/track` (hoặc `http://localhost:8082/api/track`)
* **Interactive UI:** `http://159.223.41.98:8082/api/ui` (hoặc `http://localhost:8082/api/ui`)
