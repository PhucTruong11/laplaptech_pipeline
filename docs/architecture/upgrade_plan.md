# 🚀 LapLapTech Pipeline — Upgrade & Learning Plan

> **Mục tiêu:** Phân tích từng concept, đánh giá mức độ phù hợp với project hiện tại,
> và đề xuất plan cụ thể. Mỗi concept: *"Nó là gì?"*, *"Nên/Không nên"*, *"Lý do cụ thể"*.

---

## 📋 Tổng quan đánh giá nhanh

| Concept | Nên áp dụng? | Độ ưu tiên | Độ phức tạp |
|---|---|---|---|
| Late-arriving data, Deduplication | ✅ Nên | HIGH | Trung bình |
| CDC (Change Data Capture) | ⚠️ Học để biết | LOW | Cao |
| MERGE / Upsert | ✅ Nên | HIGH | Trung bình |
| Data Quality (NOT NULL, UNIQUE...) | ✅ Nên ngay | HIGH | Thấp |
| dbt Tests mở rộng | ✅ Nên ngay | HIGH | Thấp |
| dbt Docs | ✅ Nên ngay | MEDIUM | Thấp |
| dbt Snapshots | ⚠️ Học để biết | LOW | Trung bình |
| dbt Macros | ✅ Nên | MEDIUM | Trung bình |
| dbt Seeds | ✅ Nên (có use case) | MEDIUM | Thấp |
| dbt Exposures | ✅ Nên | LOW | Thấp |
| Raw Layer Architecture | 📖 Phân tích sâu | — | — |
| Bronze Layer Scale-up | 📖 Phân tích sâu | — | — |
| Silver Normalization / Joining | ⚠️ Có điều kiện | LOW | Cao |
| BI Tool thay thế Streamlit | ⚠️ Học để biết | LOW | Cao |
| Idempotency | ✅ Đang tốt, cải thiện | MEDIUM | Thấp |

---

## 1. 🔄 Full Refresh vs Incremental

**Khái niệm:**
- **Full Refresh:** Mỗi lần chạy, xóa toàn bộ bảng cũ và nạp lại từ đầu.
- **Incremental:** Chỉ lấy thêm dữ liệu mới kể từ lần chạy gần nhất (dựa vào *watermark*).

**Trạng thái hiện tại:** Full Refresh cho 5 bảng metadata + Incremental + Watermark cho bảng events. **Đúng hướng.**

---

### 1a. Late-Arriving Data

**Nó là gì?** Sự kiện xảy ra hôm qua nhưng log chỉ ghi vào ClickHouse hôm nay (lag mạng, retry). Watermark hiện tại sẽ bỏ lỡ chúng mãi mãi.

**✅ Nên áp dụng — thêm lookback window:**

```python
# ingestion script
WHERE created_at > (max_ts - INTERVAL '60 minutes')
```

```sql
-- dbt silver incremental model
WHERE event_timestamp >= (
    SELECT MAX(event_timestamp) - INTERVAL '1 hour' FROM {{ this }}
)
```

---

### 1b. Deduplication

**Nó là gì?** Lookback window hoặc retry khiến 1 record bị ingest 2 lần. Dedup loại bỏ bản trùng.

**✅ Nên áp dụng — ROW_NUMBER() ở Silver:**

```sql
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY session_id, event_name, event_timestamp
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ source('raw', 'user_event_tracking') }}
)
SELECT * FROM deduped WHERE rn = 1
```

---

### 1c. CDC (Change Data Capture)

**Nó là gì?** Theo dõi thay đổi ở cấp DB (INSERT/UPDATE/DELETE) và stream realtime. Tools: Debezium, AWS DMS.

**⚠️ Không nên cho project này:**
- Yêu cầu quyền `replication` trên DB nguồn.
- Dự án batch hàng ngày → CDC là overkill (phù hợp khi cần latency < 1 phút).
- Cần Kafka → độ phức tạp vận hành cao.
- **Giá trị:** Hiểu CDC để áp dụng ở môi trường doanh nghiệp sau.

---

### 1d. MERGE / Upsert

**Nó là gì?** Nếu record đã tồn tại → UPDATE, chưa có → INSERT. Không cần xóa và viết lại.

**✅ Nên áp dụng — cấu hình dbt:**

```yaml
# dbt_project.yml
models:
  laplaptech_pipeline:
    silver:
      +materialized: incremental
      +incremental_strategy: merge
      +unique_key: laptop_model_id
```

Khi thông số laptop thay đổi, dbt tự UPDATE thay vì tạo bản ghi trùng.

---

## 2. 🛡️ Data Quality

**Khái niệm:** Đảm bảo dữ liệu **chính xác, đầy đủ, nhất quán, kịp thời**. Hai loại:
- **Constraint-level:** NOT NULL, UNIQUE, FK.
- **Business-level:** "Session count không thể âm".

**Trạng thái hiện tại:** Có dbt tests cơ bản. Chưa có freshness check, schema validation.

---

### 2a. NOT NULL & UNIQUE — dbt Generic Tests

**✅ Nên mở rộng ngay:**

```yaml
- name: silver_user_event_tracking
  columns:
    - name: session_id
      tests:
        - not_null
        - unique
    - name: event_name
      tests:
        - not_null
        - accepted_values:
            values: ['page_view', 'search_for_device', 'view_detail', 'compare_device']
```

---

### 2b. FOREIGN KEY — Relationships Test

**✅ Đang có, cần mở rộng:**

```yaml
- name: silver_laptop_model
  columns:
    - name: cpu_id
      tests:
        - relationships:
            arguments:
              to: ref('silver_cpu_model')
              field: cpu_id
```

---

### 2c. Freshness Check

**Nó là gì?** Cảnh báo tự động nếu dữ liệu nguồn không được cập nhật đúng giờ.

**✅ Nên áp dụng:**

```yaml
# sources.yml
sources:
  - name: raw
    freshness:
      warn_after: {count: 25, period: hour}
      error_after: {count: 48, period: hour}
    loaded_at_field: created_at
    tables:
      - name: user_event_tracking
```

Chạy: `dbt source freshness`

---

### 2d. Schema Validation

**Nó là gì?** Phát hiện khi bảng nguồn thêm/xóa/đổi cột không báo trước.

**⚠️ Nên hiểu, dùng `dbt-expectations` khi cần:**

```yaml
- name: laptop_model_id
  tests:
    - dbt_expectations.expect_column_values_to_be_of_type:
        column_type: integer
```

---

## 3. 🔧 dbt — Các tính năng chưa dùng

### 3a. dbt Docs

**✅ Nên áp dụng ngay — Zero effort, CV point cao:**

```yaml
- name: user_event_tracking
  description: "Toàn bộ hành vi người dùng: page_view, search, compare."
  columns:
    - name: session_id
      description: "ID phiên làm việc duy nhất của người dùng"
```

```bash
dbt docs generate && dbt docs serve
```

---

### 3b. dbt Snapshots (SCD Type 2)

**Nó là gì?** Lưu **lịch sử thay đổi** của dữ liệu. Ví dụ: track biến động giá laptop.

**⚠️ Học để biết — chưa có use case (không có cột price để track):**

```sql
{% snapshot snapshot_laptop_price %}
  {{ config(target_schema='snapshots', unique_key='laptop_model_id',
            strategy='check', check_cols=['price_vnd']) }}
  SELECT * FROM {{ source('raw', 'laptop_model') }}
{% endsnapshot %}
```

---

### 3c. dbt Macros

**Nó là gì?** Giống function trong Python — viết một lần, dùng nhiều model.

**✅ Nên áp dụng:**

```sql
-- macros/clean_string.sql
{% macro clean_string(column_name) %}
    TRIM(LOWER(REPLACE({{ column_name }}, '  ', ' ')))
{% endmacro %}

-- Silver model
SELECT {{ clean_string('laptop_name') }} AS laptop_name FROM ...
```

---

### 3d. dbt Seeds

**Nó là gì?** Tải CSV nhỏ thành bảng DB bằng `dbt seed`. Dùng cho lookup tables tĩnh.

**✅ Nên áp dụng — giải pháp cho Price Analytics:**

```
seeds/laptop_manual_price.csv
laptop_name,price_vnd
"Apple Macbook Air M3 13",28990000
"Dell XPS 15",45000000
```

```bash
dbt seed  # -> Tạo bảng raw.laptop_manual_price trong Supabase
```

---

### 3e. dbt Exposures

**Nó là gì?** Khai báo cho dbt biết dữ liệu Gold đang được dùng ở đâu (Streamlit...). Lineage Graph sẽ hoàn chỉnh từ nguồn đến điểm cuối.

**✅ Nên áp dụng:**

```yaml
# models/exposures.yml
exposures:
  - name: laplaptech_streamlit_dashboard
    type: dashboard
    maturity: medium
    owner:
      name: Truong Trong Phuc
    description: "Dashboard phân tích hiệu năng, thị trường, hành vi người dùng."
    depends_on:
      - ref('mart_brand_interest')
      - ref('mart_performance_ranking')
      - ref('mart_performance_efficiency')
      - ref('mart_search_analytics')
```

---

## 4. 🏗️ Raw Layer — Phân tích Architectural Thinking

### Ưu điểm (Tại sao nên giữ)

| Lợi ích | Giải thích |
|---|---|
| **Reproducibility** | Logic sai → chạy lại `dbt build` từ Raw mà không cần crawl lại ClickHouse. |
| **Debugging** | So sánh Raw vs Silver vs Gold để tìm chính xác lỗi xảy ra ở đâu. |
| **Source Preservation** | ClickHouse có thể thay đổi schema/xóa data. Raw là bản sao an toàn. |
| **Separation of Concerns** | Ingestion lo "kéo data", dbt lo "xử lý". Dễ debug, test, maintain. |

### Nhược điểm

| Hạn chế | Giải thích |
|---|---|
| **Storage** | Lưu Raw + Bronze + Silver → tốn 2-3x. Với project nhỏ không đáng kể. |
| **Latency** | Thêm bước landing → chậm hơn vài phút. |

> **Kết luận: Giữ nguyên kiến trúc. Lợi ích vượt trội nhược điểm ở quy mô hiện tại.**

---

## 5. 🥉 Bronze Layer — Scale-up Thinking

**Trạng thái hiện tại:** `SELECT *` từ Raw dưới dạng View — đúng vai trò abstraction layer.

**Nếu scale up:**

**Nhiều nguồn dữ liệu:**
```sql
SELECT 'laplaptech' AS source, * FROM {{ source('raw', 'laptop_model') }}
UNION ALL
SELECT 'tgdd' AS source, * FROM {{ source('raw_tgdd', 'products') }}
```

**Team lớn hơn:** Bronze là "contract" giữa team Ingestion và Analytics.

**Data lớn:** Đổi Bronze từ `VIEW` → `TABLE` hoặc `INCREMENTAL` để query nhanh hơn.

---

## 6. 🥈 Silver Layer — Normalization, Joining, Parsing

| Tính năng | Nên thêm? | Lý do |
|---|---|---|
| **Normalization** | ⚠️ Không cần | Silver đã normalize tốt, thêm là over-engineering. |
| **Joining** | ❌ Không nên | JOIN ở Silver tăng coupling → khi 1 bảng thay đổi kéo theo nhiều model vỡ. JOIN nên ở Gold. |
| **Parsing** | ✅ Đang làm tốt | Parse JSON ở Silver là chuẩn. Tiếp tục. |

> **Kết luận: Silver đang ở "sweet spot". Không nên thêm phức tạp.**

---

## 7. 📊 Streamlit vs BI Tool

| Tiêu chí | Streamlit | Metabase / Lightdash |
|---|---|---|
| Tùy biến UI | ★★★★★ | ★★★ |
| Business user tự dùng | ★ | ★★★★★ |
| CV value (Data Engineer) | ★★★★★ | ★★★ |
| Kỹ năng yêu cầu | Python | SQL cơ bản |

**Gợi ý:** Giữ Streamlit + thêm Metabase (free, self-hosted, kết nối Supabase dễ) như optional layer để demo.

---

## 8. ⚙️ Idempotency

**Nó là gì?** Chạy pipeline 1 lần hay 10 lần → **cùng kết quả**. Không duplicate, không mất data.

**Trạng thái:**
- Full Refresh: ✅ Hoàn toàn idempotent.
- Incremental: ⚠️ Chạy lại cùng khoảng thời gian → có thể duplicate.

**✅ Cải thiện:** Kết hợp Deduplication (mục 1b) + `unique_key` trong dbt incremental.

---

## 📅 Roadmap 3 Sprints

### 🟢 Sprint 1 — Fast Wins & Data Governance ✅ (ĐÃ HOÀN THÀNH)
- [x] Mở rộng `model_tests.yml`: `accepted_values`, `relationships` cho Silver (Hoàn thành: **26/26 tests PASS**)
- [x] Khai báo `exposures.yml` cho Streamlit Dashboard (**1 exposure** kết nối 12 bảng Gold)
- [x] Thêm `freshness` check vào `sources.yml` (**1/1 PASS**, cảnh báo 24h/48h)
- [x] Chạy `dbt docs generate` & `dbt docs serve`, kiểm tra Lineage Graph & Documentation

#### 💻 Hướng dẫn chạy nhanh Sprint 1 (Cheatsheet Commands)
Khi cần chạy kiểm thử hoặc xem tài liệu dbt, mở terminal PowerShell và thực hiện:

```powershell
# 1. Di chuyển vào thư mục dbt và kích hoạt venv
cd d:\Dev\Project\laplaptech_pipeline\dbt
..\venv\Scripts\activate

# 2. Kiểm tra Data Quality (Chạy 26 tests ràng buộc toàn vẹn & giá trị hợp lệ)
dbt test

# 3. Kiểm tra độ tươi của dữ liệu nguồn (Source Freshness)
dbt source freshness

# 4. Sinh tài liệu và khởi chạy giao diện web Lineage Graph
dbt docs generate
dbt docs serve --port 8080
```

---

### 🟢 Sprint 2 — Core Data Engineering & Idempotency ✅ (ĐÃ HOÀN THÀNH)
- [x] **2.1 Tối ưu Ingestion Script (`clickhouse_to_postgres.py`)**: Sửa lỗi phá vỡ view (đổi `DROP CASCADE` sang `TRUNCATE` + `append`) & Thêm **Lookback window (-1h)** bắt dữ liệu trễ.
- [x] **2.2 Deduplication (`ROW_NUMBER()`)**: Loại bỏ các dòng sự kiện trùng lặp do cơ chế Lookback window sinh ra trong model `silver_user_event_tracking`.
- [x] **2.3 dbt Macros (`clean_string`, `safe_divide`)**: Tránh lỗi chia cho 0 và tái sử dụng logic làm sạch chuỗi. Đã áp dụng đồng bộ cho 5 models.
- [x] **2.4 Incremental Models**: Chuyển đổi `silver_user_event_tracking` thành bảng Incremental, giúp giảm thời gian parse JSON hàng triệu dòng từ vài phút xuống vài giây.

#### 💻 Hướng dẫn chạy toàn bộ Hệ Thống (Pipeline Cheatsheet)
Để chạy toàn bộ Data Pipeline từ việc kéo dữ liệu đến khi transform ra các bảng Data Mart cuối cùng, mở terminal PowerShell và thực hiện:

```powershell
# 1. Đi tới thư mục gốc và kích hoạt môi trường ảo
cd d:\Dev\Project\laplaptech_pipeline
.\venv\Scripts\activate

# 2. Chạy Script kéo dữ liệu từ ClickHouse về PostgreSQL
python ingestion\clickhouse_to_postgres.py

# 3. Chạy dbt để Transform dữ liệu (Dùng chế độ Incremental tự động)
cd dbt
dbt run

# (Tùy chọn) 4. Mở Dashboard Streamlit để xem biểu đồ
cd ..
streamlit run streamlit_app\app.py
```

---

### 🟡 Sprint 3 — Optional & Enhancements ⏳ (TIẾP THEO)
- [ ] `seeds/laptop_manual_price.csv` → Price Analytics
- [ ] Snapshot cho `laptop_model` (SCD Type 2 khi có track biến động giá)
- [ ] `dbt-expectations` cho schema validation nâng cao
- [ ] Metabase kết nối Supabase

---

> 📝 **Living document** — Cập nhật lần cuối sau khi hoàn thành Sprint 2 (26/09/2026).

