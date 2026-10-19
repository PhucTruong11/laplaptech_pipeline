# Hướng dẫn thiết kế hệ thống dữ liệu từ LaplapTech

Cập nhật theo code ngày 19/10/2026. Project dùng để học và nghiên cứu. Các sơ đồ “hiện tại” chỉ mô tả thành phần đã có code; Airflow là hướng phát triển tiếp.

Bản nâng cấp đã kiểm chứng: [Gold upgrade](docs/guides/gold_upgrade.md).

Tài liệu triển khai chi tiết: [data flow](docs/architecture/data_flow.md). Danh sách công việc: [remaining work](docs/architecture/remaining_work.md).

## 1. Bắt đầu từ yêu cầu

Trước khi chọn công cụ, xác định:

| Câu hỏi | Áp dụng cho project |
|---|---|
| Volume | Nguồn từng được ghi nhận khoảng 760k+ event; cần đo số mới theo run |
| Velocity | Batch hai lần/ngày, không có yêu cầu realtime đã được triển khai |
| Variety | Danh mục/benchmark dạng bảng và event chứa JSON |
| Veracity | ID/version, dữ liệu NULL, JSON lỗi, event trễ và grain của metric |
| Mục tiêu | Học pipeline end-to-end, thử tình huống lỗi và giải thích kết quả |

Số công cụ không phải thước đo duy nhất của engineering. Thiết kế có ý nghĩa khi giải thích được invariant, failure path, replay và bằng chứng kiểm thử.

## 2. Luồng đang triển khai

![Data flow: ClickHouse → ingestion → Neon raw/Bronze/Silver/Gold → Streamlit](assets/data-flow.svg)

dbt CLI chạy trên runner; SQL được thực thi trong PostgreSQL Neon, gồm cả việc tạo Bronze. Streamlit được khởi chạy riêng; job sync không deploy app. Pipeline áp dụng ELT với phân lớp raw/Bronze/Silver/Gold.

## 3. Thiết kế ingestion

### Watermark và dữ liệu đến muộn

Bảng event dùng server-received timestamp làm watermark, không dùng thời điểm client xảy ra event. Lượt thường lấy MAX raw trừ 2 giờ; Silver có watermark riêng và cửa sổ 2 giờ.

Event xảy ra từ lâu nhưng server vừa nhận có thể vẫn được lấy bình thường. Ngược lại, bản xuất hiện ở nguồn sau đó mà giữ timestamp quá cũ có thể nằm ngoài lookback. Vì vậy phải có backfill, không khẳng định lookback xử lý được mọi trường hợp.

CLI backfill đã có `--start` và `--end`, khoảng đầu bao gồm/cuối không bao gồm. Nó mới giới hạn extraction event; dimension vẫn là snapshot hiện tại.

### Atomicity và idempotency

```mermaid
%%{init: {"flowchart": {"htmlLabels": false}}}%%
flowchart TD
    FULL["Dimension DataFrame"] --> FTX["BEGIN -> lock -> schema check -><br/>TRUNCATE -> insert -> COMMIT"]
    EVENT["Event DataFrame"] --> DEDUP["Chọn version mới nhất theo ID"]
    DEDUP --> ETX["BEGIN -> lock -> staging UUID -><br/>version check -> DELETE/INSERT -><br/>DROP staging -> COMMIT"]
    FTX -->|"Bất kỳ lỗi trong transaction"| ROLLBACK["ROLLBACK transaction bảng đang nạp"]
    ETX -->|"Bất kỳ lỗi trong transaction"| ROLLBACK
```

- TRUNCATE giữ view, nhưng phải dùng cùng Connection/transaction với insert để rollback được dữ liệu.
- Staging riêng ngăn job xóa bảng tạm của job khác. Transaction lock bảo vệ writer cùng quy tắc.
- Replay event không tăng bản ghi theo ID trong trường hợp đã kiểm thử; bản cũ không thay bản mới.
- Cùng ID/timestamp nhưng payload khác phải điều tra, vì timestamp không đủ phân xử version.
- Chưa có transaction bao trùm toàn bộ 6 bảng và dbt; bảng đã commit trước lỗi vẫn giữ dữ liệu mới.
- Raw lưu version được chọn, không phải immutable event history; xóa bản ghi ở nguồn chưa được đồng bộ bằng CDC.

Extract toàn bộ kết quả vào RAM; chia chunk lúc insert chỉ giảm kích thước câu INSERT. Muốn học scale thêm, đo memory/thời gian rồi mới cân nhắc extraction theo trang/stream.

## 4. Thiết kế warehouse và transformation

Neon giữ database PostgreSQL; không dùng nó làm scheduler. Với profile `schema: public` và custom schema trong dbt, các tầng là:

| Schema | Materialization | Vai trò |
|---|---|---|
| `raw` | 6 tables do ingestion quản lý | Snapshot danh mục và event upsert |
| `public_bronze` | 6 views | Ánh xạ nguồn 1:1; chưa enforce data contract |
| `public_silver` | 12 views + 1 incremental table | Kiểu dữ liệu, JSON, catalog enrichment, session/event views |
| `public_gold` | 12 tables | Aggregate và join phục vụ phân tích |

Tầng Bronze không tự làm sạch và `SELECT *` không phải schema contract. Loader hiện chặn thêm/bớt tên cột ở bảng đã có; kiểm tra type/schema evolution đầy đủ vẫn là việc tiếp theo.

Gold hiện rebuild theo model, không phải incremental. DataFrame/SQL source không tự tạo unique index chỉ vì dbt có `unique_key`; data tests và constraint là hai lớp khác nhau.

### Grain phải đi trước metric

Daily KPI mới là một ngày server nhận event theo giờ Việt Nam. Session qua nửa đêm có thể tính ở hai ngày; SUM daily sessions là session-days.

Funnel và daily KPI cùng dùng session-day, đo reach độc lập thay vì sequential conversion. CPU/GPU dùng business_month theo giờ Việt Nam và giữ cả nhóm ít view. Không cộng/chia các aggregate khác grain để suy ra conversion mà chưa kiểm tra tập session.

Silver chọn benchmark ID lớn nhất mỗi laptop để tránh fanout; đây là quy tắc đại diện, không suy ra thời gian. Brand pair được canonicalize và đo co-occurrence của comparison interactions trong session-day.

## 5. Orchestration và kiểm soát chất lượng

```mermaid
sequenceDiagram
    participant GH as GitHub Actions
    participant PY as Python ingestion
    participant PG as Neon PostgreSQL
    participant DBT as dbt CLI
    GH->>PY: Trigger main/manual main hoặc lịch
    PY->>PG: Commit từng bảng raw
    alt Ingestion thành công
        GH->>DBT: source freshness
        DBT->>PG: Đọc MAX server timestamp
        alt Không có freshness error
            GH->>DBT: build models và data tests
            DBT->>PG: SQL theo DAG
            DBT-->>GH: Kết quả build/test, có thể thành công hoặc fail
        else Freshness error
            DBT-->>GH: Fail, không build
        end
    else Ingestion fail
        PY-->>GH: Exit khác 0, bỏ qua freshness/build
    end
    GH->>GH: Lưu artifact dbt đã có bằng always
```

Freshness warn 24 giờ/error 48 giờ được chạy riêng trước build. MAX timestamp mới không chứng minh mọi ID/version đã đầy đủ. Chưa có bảng audit phân biệt một run thành công không có traffic mới với job không chạy.

Có 47 data test (41 generic + 6 singular) đã pass trên PostgreSQL dev local với fixture và nguồn ClickHouse thật. Test nguồn/Silver ID và daily event count hỗ trợ invariant; đã đối soát raw-Silver ID/version và aggregate totals; quarantine row lỗi còn là việc tiếp theo. Exposure chỉ là metadata lineage.

## 6. Tách kiểm chứng code khỏi dữ liệu thật

```mermaid
%%{init: {"flowchart": {"htmlLabels": false}}}%%
flowchart LR
    PR["PR / develop push"] --> COMPILE["Compile Python"]
    COMPILE --> TEST["17 unit/helper + 9 PostgreSQL<br/>integration tests"]
    TEST --> PARSE["dbt parse: credential giả"]
    PG["PostgreSQL 16 CI service"] --> TEST
```

28 Python test pass, gồm 9 ingestion integration và 2 AppTest. 31 model build và 47 data test pass trên dev với 784.455 raw event; replay giữ nguyên row counts. Chưa deploy Neon.

Môi trường CI không cần secrets dữ liệu thật. 85 runtime dependency đã pin trong lock, cài sạch và pip check pass trên Windows/Linux Python 3.13.

## 7. Serving cũng cần kiểm tra tính đúng

App đọc đủ 12 Gold model, cache 300 giây; exposure khớp 12 dependency. Nếu build cập nhật được một số mart rồi fail, app vẫn có thể đọc các version khác nhau. Chưa có publish snapshot đồng loạt.

Dashboard mới dùng Today thật, nhãn session-days, benchmark N/A và literal search; có bộ lọc ngày và latest received event. Xem [backlog có tiêu chí nghiệm thu](docs/architecture/remaining_work.md).

## 8. Airflow Docker cho giai đoạn học tiếp

```mermaid
%%{init: {"flowchart": {"htmlLabels": false}}}%%
flowchart LR
    AF["Airflow Docker trên laptop: ingest<br/>-> freshness -> build"] --> NEON["Neon warehouse"]
    AF -.-> META["Metadata PostgreSQL riêng trong<br/>Docker"]
    GHA["GitHub Actions"] --> CI["CI/tests sau khi chuyển lịch"]
```

Đây là thiết kế dự kiến, chưa có DAG/runtime triển khai trong repo. Có thể giữ Neon khi chuyển nơi điều phối sang laptop. Chưa bắt buộc VPS nếu chấp nhận chỉ chạy khi máy/Docker bật.

Bài học nên thực hành: retry không nhân dữ liệu, rerun dbt riêng, timeout, không chạy chồng, failed task và backfill theo data interval. Bounded ingestion đã có, nhưng Silver ngoài lookback và mapping interval trong DAG vẫn cần hoàn thiện.

Chuyển lịch theo thứ tự dừng writer cũ rồi bật writer mới. Giữ GitHub Actions cho validation. Chưa có nhu cầu đã chứng minh để thêm Kafka/Spark/Kubernetes vào project này.

## 9. Mở rộng khi đã có bằng chứng nhu cầu

- Nhiều nguồn/dependency: thêm DAG/task boundaries và audit theo run.
- Dataset vượt khả năng RAM: đo bottleneck, thử chunk extraction và checkpoint có kiểm thử.
- Cần lịch sử thay đổi danh mục: thiết kế snapshot/SCD, không lấy snapshot hiện tại làm lịch sử.
- Cần đồng bộ delete/update từ nguồn: chọn version/change signal phù hợp, thay vì chỉ tăng lookback.
- Cần phục vụ báo cáo nhất quán theo run: thiết kế publish/version của tập mart.
- Cần production liên tục: đánh giá hạ tầng thường trực, secret management và giám sát riêng.

Các mục này là hướng thiết kế; không phải tính năng project đã hoàn thành.
