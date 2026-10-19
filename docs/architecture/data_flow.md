# Data flow hiện tại của LaplapTech

Cập nhật ngày 19/10/2026, đối chiếu source trong repository. Đây là kiến trúc đã có code; chưa xác nhận các thay đổi mới đã chạy end-to-end trên Neon. Airflow nằm ở phần kế hoạch, chưa thay GitHub Actions.

## 1. Các thành phần chạy ở đâu?

| Thành phần | Nơi chạy/lưu | Trách nhiệm |
|---|---|---|
| ClickHouse | Nguồn bên ngoài | 5 bảng danh mục/benchmark và 1 bảng event |
| Python ingestion | GitHub Actions runner hoặc máy local | Extract vào DataFrame, nạp PostgreSQL raw |
| dbt CLI | Cùng runner/máy local | Lập DAG, gửi SQL và chạy data tests |
| SQL transformation | PostgreSQL Neon | Thực thi view, incremental và tạo Gold tables |
| Warehouse | Neon | `raw`, `public_bronze`, `public_silver`, `public_gold` |
| Streamlit | Nơi người dùng khởi chạy app | Truy vấn Gold, cache kết quả 300 giây |
| PostgreSQL test | Docker local hoặc CI service | Dữ liệu fixture và schema tạm để chạy integration tests |

Workflow hiện tại không deploy hoặc restart Streamlit. Neon lưu dữ liệu; GitHub Actions cung cấp nơi chạy job. Hai vai trò này độc lập.

## 2. Luồng dữ liệu tổng thể

Mũi tên liền là dữ liệu/quan hệ phụ thuộc; mũi tên đứt là điều khiển hoặc metadata.

```mermaid
flowchart TD
    CH["ClickHouse: 6 bảng nguồn"]
    GH["GitHub Actions: main, manual trên main, lịch 2 lần/ngày"]
    PY["Python ingestion trên runner"]
    CLI["dbt CLI trên runner"]

    subgraph NEON["PostgreSQL warehouse trên Neon"]
        RAW["raw: 5 full-refresh tables và 1 event table"]
        B["public_bronze: 6 views ánh xạ raw"]
        S["public_silver: 10 views và 1 incremental event table"]
        G["public_gold: 12 mart tables"]
        RAW --> B
        B --> S
        S --> G
    end

    CH -->|"Extract full hoặc watermark/backfill"| PY
    PY -->|"Transaction theo từng bảng"| RAW
    GH -.-> PY
    GH -.-> CLI
    CLI -.->|"Freshness, build và data tests"| NEON
    G -->|"8 mart được app truy vấn trực tiếp"| APP["Streamlit và cache 300 giây"]
    EXP["dbt exposure: khai báo dependency vào 12 mart"] -.-> APP
```

Bronze là pass-through view, chưa phải data contract được enforce. Raw giữ snapshot danh mục và phiên bản event được chọn theo ID; raw không lưu bất biến mọi phiên bản nguồn.

## 3. Control flow của job cập nhật

```mermaid
flowchart LR
    T["main push / manual main / 08:15 và 16:45 VN"] --> Q["Concurrency group theo warehouse"]
    Q --> I["Ingestion"]
    I -->|"Thành công"| F["dbt source freshness"]
    F -->|"Không có freshness error"| D["dbt build: models và tests theo DAG"]
    I -->|"Thất bại"| STOP["Không chạy các bước xử lý phía sau"]
    F -->|"Trễ quá 48 giờ"| STOP
    D -->|"Model/test lỗi"| FAIL["Job fail; downstream có thể bị skip"]
    I -.-> A["Upload artifact: always"]
    F -.-> A
    D -.-> A
```

- Lịch cấu hình UTC: `15 1 * * *` và `45 9 * * *`, tương ứng 08:15 và 16:45 Việt Nam. Đây là lịch cấu hình, không phải cam kết job luôn bắt đầu đúng phút.
- `concurrency.group` cố định và `cancel-in-progress: false` tránh hai run trong cùng group chạy đồng thời. Không coi cơ chế này là hàng đợi giữ mọi trigger.
- Freshness dùng MAX server timestamp của event: warn sau 24 giờ, error sau 48 giờ. Warn không chặn build theo cấu hình hiện tại.
- `dbt build` đã chạy data tests; không chạy `dbt test` toàn bộ lần nữa ngay sau đó.
- Upload giữ `manifest.json`, `run_results.json`, `sources.json` nếu đã sinh, retention 14 ngày. Ingestion lỗi sớm có thể chưa có artifact dbt.
- Chưa có automatic retry cho toàn job, bảng audit theo run hoặc notification riêng ngoài cấu hình thông báo GitHub của người dùng.

## 4. Ingestion: transaction và phiên bản dữ liệu

### 4.1 Full refresh cho 5 bảng

Áp dụng cho `brand`, `cpu_model`, `gpu_model`, `laptop_model`, `laptop_benchmark_result`.

```mermaid
flowchart TD
    E["SELECT toàn bảng từ ClickHouse"] --> V{"DataFrame có dữ liệu?"}
    V -->|"Không"| REJECT["Fail và giữ bảng cũ"]
    V -->|"Có"| TX["BEGIN và transaction advisory lock theo bảng"]
    TX --> CHECK["Kiểm tra bảng tồn tại và tập tên cột"]
    CHECK --> TR["TRUNCATE nếu đã có bảng"]
    TR --> W["to_sql append bằng cùng Connection"]
    W --> C["COMMIT"]
    CHECK -->|"Schema thay đổi"| RB["ROLLBACK"]
    W -->|"Insert lỗi"| RB
```

TRUNCATE giữ định nghĩa view. Atomicity đến từ việc TRUNCATE và insert dùng cùng transaction; chỉ TRUNCATE rồi append ở hai transaction khác nhau không bảo đảm rollback dữ liệu.

`to_sql` dùng chunk tối đa 1.000 dòng và giới hạn theo số cột. Extraction vẫn giữ toàn bộ kết quả trong RAM; chunk insert không đồng nghĩa streaming extraction.

### 4.2 Event incremental

Watermark: `event_received_on_server_timestamp`, dạng epoch giây. Lượt thông thường lấy `timestamp >= MAX(raw timestamp) - INGESTION_LOOKBACK_SECONDS`, mặc định 7.200 giây. Chưa có bảng hoặc chưa có watermark thì extract toàn bảng. Lỗi đọc watermark phải fail, không biến thành full extraction.

```mermaid
flowchart TD
    E["Extract event theo watermark hoặc khoảng backfill"] --> P["Kiểm tra ID, epoch và chuẩn hóa JSON object"]
    P --> D["Chọn timestamp lớn nhất cho mỗi ID"]
    D --> X{"Cùng ID và timestamp mới nhất nhưng payload khác?"}
    X -->|"Có"| BAD["Fail để điều tra version nguồn"]
    X -->|"Không"| TX["BEGIN và transaction advisory lock"]
    TX --> EXISTS{"Đã có bảng raw?"}
    EXISTS -->|"Chưa"| FIRST["Tạo và nạp batch đầu"]
    EXISTS -->|"Có"| STG["Tạo staging có hậu tố UUID và nạp batch"]
    STG --> OLD["Bỏ dòng staging cũ hơn bản đã có trong raw"]
    OLD --> CONFLICT["Kiểm tra payload xung đột cùng ID và timestamp với raw"]
    CONFLICT --> DEL["DELETE các ID được thay thế"]
    DEL --> INS["INSERT theo danh sách cột tường minh"]
    INS --> DROP["DROP staging trong cùng transaction"]
    FIRST --> COMMIT["COMMIT"]
    DROP --> COMMIT
    CONFLICT -->|"Xung đột"| RB["ROLLBACK cả raw và staging"]
    INS -->|"Lỗi"| RB
```

Xung đột cùng timestamp bị fail thay vì chọn bản ngẫu nhiên. Muốn hỗ trợ sửa payload có cùng server timestamp cần version/updated_at và quy tắc chọn bản rõ ràng.

Transaction lock bảo vệ các writer sử dụng cùng khóa trong code. Chưa có migration tự động thêm unique index vào raw; generic tests phát hiện duplicate không thay thế constraint vật lý. Từng bảng commit riêng; cả ingestion + dbt không phải một transaction toàn cục.

## 5. dbt lineage và grain

### 5.1 Các nhánh Silver

```mermaid
flowchart LR
    BD["Bronze brand, CPU, GPU"] --> SD["Silver brand, CPU, GPU"]
    BL["bronze_laptop_model"] --> LAP["silver_laptop_model"]
    SD --> LAP
    BB["bronze_laptop_benchmark_result"] --> BEN["silver_laptop_benchmark_result"]
    BE["bronze_user_event_tracking"] --> EVT["silver_user_event_tracking: incremental theo ID"]
    EVT --> SF["Session activity và session funnel"]
    EVT --> CE["Comparison session-device"]
    EVT --> SORT["Comparison sort event: chưa có Gold downstream"]
    EVT --> DT["Device traffic event"]
    LAP --> MART["Các Gold mart join catalog/event/benchmark"]
    BEN --> MART
    EVT --> MART
    SF --> MART
    CE --> MART
```

Silver event parse JSON, đổi epoch sang timestamp có timezone, lọc event_name/session_id NULL và dedup theo ID. Incremental đọc lại cửa sổ 2 giờ từ MAX timestamp của **Silver**. Đây là watermark riêng, không phải MAX raw. `unique_key='id'` của dbt không tự tạo unique constraint.

`silver_comparison_sort_event` và `silver_device_traffic_event` hiện không có Gold model downstream; chúng vẫn là view chuẩn bị cho phân tích mở rộng. `mart_daily_site_kpis` đọc trực tiếp Silver event, không còn lấy COUNT từ `silver_session_funnel`.

### 5.2 12 Gold mart

| Model | Upstream Silver | Grain và ý nghĩa hiện tại |
|---|---|---|
| `mart_daily_site_kpis` | user_event_tracking | Một ngày Việt Nam, đo active session-days và event thực tế |
| `mart_behavior_funnel_daily` | session_funnel | Ngày bắt đầu session; đếm các bước đã xuất hiện, không yêu cầu thứ tự |
| `mart_brand_interest` | user_event_tracking, laptop_model, comparison_session_device | Một brand có detail view; số view và session quan tâm |
| `mart_brand_comparison` | comparison_session_device, laptop_model | Cặp brand theo hướng laptop_a/laptop_b; suy từ đồng xuất hiện trong session |
| `mart_cpu_trend` | user_event_tracking, laptop_model | Tháng, CPU, brand; chỉ detail pageview, nhóm ít nhất 5 view |
| `mart_gpu_trend` | user_event_tracking, laptop_model | Tháng, GPU, brand; chỉ detail pageview, nhóm ít nhất 5 view |
| `mart_search_analytics` | user_event_tracking | Keyword đã lower/trim, tổng search và distinct sessions |
| `mart_spec_popularity` | user_event_tracking, laptop_model | CPU, GPU, kích thước màn hình, dung lượng pin; chưa có RAM/storage |
| `mart_performance_ranking` | laptop_model, laptop_benchmark_result | Laptop active join benchmark; cần kiểm chứng một benchmark/laptop |
| `mart_performance_efficiency` | laptop_model, laptop_benchmark_result | Laptop active/visible join benchmark có điểm CPU; hiệu năng trên kg/Wh |
| `mart_battery_vs_interest` | user_event_tracking, laptop_model, laptop_benchmark_result | Laptop active join benchmark và tổng interest |
| `mart_user_os` | session_activity | OS và device type, đếm session |

Gold materialize thành table, rebuild theo DAG mỗi lần build; chưa phải incremental Gold. Join benchmark có thể tạo nhiều dòng/laptop nếu upstream có nhiều benchmark cho cùng laptop. Các tỷ lệ brand interest và cặp brand vẫn cần chuẩn hóa định nghĩa; xem [việc còn lại](remaining_work.md).

### 5.3 Quy ước thời gian và metric

- `event_timestamp`: thời điểm client ghi nhận; `server_timestamp`: thời điểm server nhận.
- Daily KPI mới nhóm `server_timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh'` theo ngày.
- `total_sessions` là distinct session **trong từng ngày**; SUM nhiều ngày là session-days.
- `detail_rate_pct`: session có cả pageview và detail / session có pageview trong ngày.
- `comparison_rate_pct`: session có cả detail và comparison / session có detail trong ngày.
- Hai tỷ lệ đo cùng đạt bước, không chứng minh thứ tự chuyển đổi.
- Session funnel cũ gán về ngày bắt đầu session; CPU/GPU và một số view vẫn phụ thuộc timezone PostgreSQL khi cast ngày/tháng. Chưa đồng bộ toàn bộ về giờ Việt Nam.

## 6. Serving và exposure

App truy vấn trực tiếp 8 mart: daily KPI, brand interest, CPU trend, GPU trend, performance ranking, performance efficiency, battery vs interest và search analytics.

Exposure khai báo dependency vào cả 12 mart; đó là lineage metadata, không chứng minh UI hiện sử dụng đủ 12 mart. Cache query 300 giây có thể giữ kết quả trước lượt build vừa xong. Chưa có publish/swap đồng loạt toàn bộ Gold, nên khi build lỗi giữa chừng app có thể thấy các mart từ những run khác nhau.

Dashboard chưa sửa nhãn Today/All-Time và benchmark fallback. Dữ liệu đúng ở mart chưa bảo đảm người xem đọc đúng ý nghĩa trên UI.

## 7. Backfill và phục hồi

```mermaid
flowchart LR
    R["Chọn start và end có timezone"] --> E["Extract theo server timestamp trong khoảng start-inclusive, end-exclusive"]
    E --> U["Upsert raw; dimension refresh về hiện tại"]
    U --> W{"Dữ liệu cũ nằm ngoài cửa sổ Silver?"}
    W -->|"Có"| REBUILD["Rebuild hoặc backfill Silver và downstream trong dev trước"]
    W -->|"Không"| BUILD["Freshness và dbt build thông thường"]
    REBUILD --> VERIFY["Đối soát ID, version và metric"]
    BUILD --> VERIFY
```

CLI `--start`/`--end` đã có trong ingestion. Full-refresh dbt chỉ đọc raw đang có, không truy vấn lại ClickHouse. Lookback hữu hạn có thể bỏ sót bản xuất hiện ở nguồn với timestamp quá cũ; hai cửa sổ 2 giờ không phải bảo đảm không mất dữ liệu.

Lệnh chạy, lưu ý Neon và database test: [ingestion operations](../guides/ingestion_operations.md).

## 8. CI và bằng chứng kiểm thử

```mermaid
flowchart LR
    PR["Pull request hoặc push develop"] --> CI["validate.yml trên runner"]
    CI --> PG["PostgreSQL 16 service test riêng"]
    PG --> UT["12 unit/config tests và 9 integration tests"]
    UT --> PARSE["dbt parse bằng cấu hình giả, không kết nối warehouse"]
```

Validate còn compile Python trước khi chạy tests. Không cần credential Neon/ClickHouse. Mỗi integration test tạo schema UUID và dọn đúng schema đó trong database local test.

Kết quả đã ghi nhận ngày 19/10/2026: 21/21 Python test pass; `dbt parse` nhận 29 model, 34 data test, 6 source và 1 exposure. Trong 34 data test có 32 generic và 2 singular. Freshness là lệnh riêng; exposure không tính là test. Chưa ghi nhận 34 data test pass trên Neon sau đợt sửa.

## 9. Airflow Docker: hướng học tiếp, chưa triển khai

```mermaid
flowchart LR
    subgraph LOCAL["Laptop - Docker: kế hoạch học tiếp"]
        AF["Airflow: ingest -> freshness -> dbt build"]
        META["PostgreSQL metadata riêng"]
        AF -.-> META
    end
    SRC["ClickHouse"] --> AF
    AF --> N["Neon warehouse giữ nguyên"]
    GIT["GitHub Actions"] --> CI["Chỉ validate và tests sau khi chuyển lịch"]
```

Neon có thể tiếp tục giữ warehouse; chưa cần VPS nếu chấp nhận job chỉ chạy khi laptop/Docker hoạt động. Chỉ một orchestrator có lịch ghi cùng warehouse tại một thời điểm. Trước khi bật Airflow catchup phải nối data interval vào tham số backfill; script có tham số chưa đồng nghĩa DAG đã hỗ trợ backfill.

Xem [upgrade plan](upgrade_plan.md) và [backlog ưu tiên](remaining_work.md) để phân biệt code đã có với việc dự kiến.
