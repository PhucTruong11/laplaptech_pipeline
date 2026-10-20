# Docker full cho cộng đồng — chặng cuối của roadmap

Cập nhật 20/10/2026. **Làm sau Airflow dùng chung và các nâng cấp engineering cốt lõi.** Đây là kế hoạch phát hành, chưa có bộ Compose/Dockerfile/bootstrap full pipeline. Bộ cấu hình hạ tầng hiện có chỉ dựng platform và DAG kiểm tra nền tảng. Xem [roadmap](../architecture/upgrade_plan.md), [backlog](../architecture/remaining_work.md) và [triển khai Airflow làm trước](airflow_setup.md).

## 1. Mục tiêu đã chốt

Người khác clone repo, cấu hình tài khoản ClickHouse public do dataset công bố, rồi chạy toàn bộ phần ứng dụng và warehouse của họ bằng Docker. Họ dùng cùng source/code pipeline, nhưng có database, metadata, logs và artifacts riêng trên máy họ.

**Neon, lịch sync GitHub Actions, validate CI và cách host dashboard hiện tại của chủ project tiếp tục giữ nguyên.** Đóng gói Docker là thêm cách tái lập dự án, không phải chuyển hệ thống đang vận hành sang laptop hoặc ngừng Actions.

ClickHouse là dịch vụ nguồn public bên ngoài Docker; bản full cần Internet để lấy nguồn thật. Fixture vẫn phục vụ CI và kiểm thử nguồn cố định, không thay nguồn public thành mặc định cho bản phát hành. Thông tin đăng nhập public phải đối chiếu với hướng dẫn dataset; không lấy credential Neon từ `.env` đang dùng.

## 2. Ba môi trường phải tách biệt

| Phần | Cloud đang vận hành | Lab cá nhân làm trước | Docker public làm cuối |
|---|---|---|---|
| Điều phối | Actions hiện tại | Platform Airflow 3.3.2 độc lập dùng chung | Airflow riêng chỉ chứa DAG LaplapTech |
| Nguồn | ClickHouse public | ClickHouse public hoặc fixture để test | ClickHouse public mặc định |
| Warehouse | Neon hiện tại | PostgreSQL dev riêng | PostgreSQL container có volume riêng |
| Metadata Airflow | Không dùng cho sync Actions | Metadata của stack chung | Metadata local riêng |
| Runtime | Runner hiện tại | Image LaplapTech Python 3.13 | Image phát hành được pin |
| Dashboard | Cách host hiện tại | Preview trỏ warehouse dev khi cần | Streamlit container |
| Cấu hình | GitHub Secrets / env hiện tại | Config lab riêng | File mẫu cấu hình local riêng |

Mỗi warehouse có writer điều phối riêng. Khi Docker của người clone tắt, phần local dừng; Actions vẫn cập nhật Neon trên GitHub-hosted runner độc lập. [GitHub-hosted runners](https://docs.github.com/en/actions/concepts/runners/github-hosted-runners).

## 3. Khi nào bắt đầu chặng này?

Hoàn thành các điều kiện hữu hạn sau; các hướng nghiên cứu như CDC, SCD hoặc Kubernetes không phải điều kiện phát hành:

- [ ] Smoke DAG và DAG LaplapTech chạy trên Airflow chung; dependency runtime tách riêng, provider tương thích được kiểm chứng.
- [ ] Manual run, replay và failure/recovery đã có logs/artifacts và kết quả đối soát.
- [ ] Chốt bootstrap schema, raw ID constraints/index và chính sách JSON lỗi/quarantine; có kiểm thử các invariant đã chọn.
- [ ] Audit tối thiểu theo run/stage/watermark và quy tắc retry/Clear/backfill được ghi rõ.
- [ ] Model/metric hiện tại, lock dependency và dashboard đã ổn định theo phạm vi release.

Các nâng cấp publish Gold đồng loạt, SCD hoặc streaming extraction chỉ đưa vào release này nếu bằng chứng nhu cầu khiến chúng trở thành blocker; nếu không, ghi giới hạn rõ trong README.

## 4. Thành phần cần đóng gói

| Thành phần | Trách nhiệm | Dữ liệu lưu bền |
|---|---|---|
| Airflow standalone | UI, scheduler và các service theo version đã chọn | Metadata PostgreSQL riêng, logs |
| Pipeline runtime | Python ingestion và dbt CLI, cùng entrypoints đã test | Artifacts theo run/task/attempt |
| Warehouse PostgreSQL | raw, Bronze, Silver, Gold; SQL transformation | Warehouse volume riêng |
| Streamlit | Dashboard đọc Gold của warehouse local | Query cache có thể tạo lại |
| Init/bootstrap | Health checks, khởi tạo metadata/schema, kích hoạt lượt build đầu | Trạng thái bootstrap có thể kiểm tra/retry |

Tái sử dụng image/CLI từ lab thay vì viết lại logic ETL trong DAG/Compose. Khi dùng DockerOperator, tiếp tục cấu hình Docker Engine/network/volumes rõ ràng; không chuyển sang giả định task chạy trong Python của scheduler. Khi chọn cách thực thi khác, giữ contract CLI và kiểm chứng tương đương.

Trước release chốt một version matrix Airflow/provider/Python/PostgreSQL và pin image/package, không dùng `latest`. Bộ cấu hình Airflow lab dùng Airflow 3.3.2/Python 3.13/LocalExecutor, PostgreSQL 16; Docker provider cho task runtime chưa được thêm. Release full cần kiểm chứng matrix và pin image digest riêng.

Compose hỗ trợ khai báo service, network và volume; named project giúp tách các lượt cài đặt độc lập. [Compose application model](https://docs.docker.com/compose/intro/compose-application-model/).

## 5. Cấu trúc file dự kiến

Danh sách dưới đây là đầu ra phải tạo ở chặng cuối; hiện chưa phải các file chạy được:

```text
compose.yaml                     # bộ standalone đầy đủ cho cộng đồng
docker/
  runtime.Dockerfile             # kế thừa runtime/CLI đã dùng cho DAG lab
  dashboard.Dockerfile           # image Streamlit
  airflow.Dockerfile             # control plane + provider đúng version
  .env.example                   # nguồn public + đích local, không chứa Neon của chủ repo
  bootstrap/                     # readiness/schema/first-run helpers có kiểm thử
airflow/dags/
  laplaptech_pipeline.py         # chỉ DAG của LaplapTech
docs/guides/
  docker_quickstart.md           # lệnh thật sau khi release được kiểm chứng
  airflow_setup.md              # khởi tạo platform và hướng tích hợp DAG
```

Override/mount của nhiều repo thuộc workspace `airflow-platform` cá nhân, không nằm trong cấu hình standalone mặc định. Image/DAG bundle phát hành không được chứa DAG hay credential/dữ liệu của các project khác. Người clone không truy cập Airflow hoặc metadata đang chạy trên máy chủ project.

## 6. Contract cấu hình và khởi tạo

### Nguồn và đích

- File mẫu điền endpoint/username và login public đã được dataset cho phép công bố; ghi nguồn và attribution.
- `POSTGRES_HOST` mặc định là service warehouse trong network Compose; `localhost` trong container không phải PostgreSQL container khác.
- Cấu hình local dùng database local riêng và SSL phù hợp; cấu hình Neon hiện tại vẫn `require`.
- `.env` là cấu hình runtime, Docker là môi trường thực thi. File cấu hình thật không COPY vào image.
- Không tự đọc root `.env` đang trỏ Neon khi người dùng chạy quickstart Docker. Có preflight xác nhận đích local trước task ghi đầu tiên.
- Tên network/volume/task image phải khớp giữa Compose, DAG và DockerOperator. Dùng project name hoặc prefix để tránh collision khi chạy nhiều bản.

### Chuỗi startup cần triển khai

1. Warehouse và metadata PostgreSQL sẵn sàng qua health checks; volumes được khởi tạo đúng owner.
2. Metadata Airflow migrate và tạo tài khoản UI local; DAG parse không có import error.
3. Kiểm tra cấu hình nguồn/đích và bootstrap warehouse bằng cơ chế đã test ở chặng engineering.
4. Kích hoạt đúng một lượt pipeline đầu tiên; không đồng thời kích hoạt từ init helper và schedule.
5. Ingestion → source freshness → dbt build/tests thành công; lưu run ID và kết quả bootstrap.
6. Dashboard chỉ báo ready sau khi có Gold hợp lệ. Khi build lỗi phải hiển thị trạng thái/lệnh xem lỗi, không biến thành KPI 0.

Chạy lại startup phải idempotent: không xóa warehouse, không tạo vòng ingestion lặp vô hạn, không xác nhận success chỉ vì một bảng Gold đã tồn tại từ lần build dở dang. Nguồn public có thể lỗi mạng hoặc stale; giữ policy freshness rõ và cho người dùng xem nguyên nhân.

## 7. Trải nghiệm quickstart cần đạt

**Các lệnh dưới đây chỉ là contract UX dự kiến; chưa chạy được khi chưa tạo bộ Docker full ở chặng này.** Sau khi kiểm chứng mới đưa thành quickstart chính thức:

```powershell
git clone https://github.com/PhucTruong11/laplaptech_pipeline.git
Set-Location laplaptech_pipeline
Copy-Item docker/.env.example docker/.env
# Kiểm tra/điền cấu hình ClickHouse public theo tài khoản dataset công bố.
docker compose --env-file docker/.env up --build -d
```

Mục tiêu: người dùng chỉ cần Docker/Compose và Internet, không phải cài Python/dbt/Airflow vào máy. README phải chỉ rõ thời gian initial load đo được, cách xem tiến độ, URL dashboard/Airflow, thao tác manual và cách dừng/khởi động lại.

Giữ bộ standalone đầy đủ là đường chạy có tài liệu chính. Người dùng đã có Airflow riêng có thể chọn tích hợp DAG/runtime theo hướng dẫn shared lab, không bắt buộc bật thêm một Airflow chỉ để thử project.

## 8. Kiểm thử trước khi công bố Docker full

| Trường hợp | Tiêu chí đạt |
|---|---|
| Clone sạch, không venv và không `.env` riêng của chủ repo | Dùng cấu hình mẫu local, kéo nguồn public và build được |
| DB/volumes rỗng | Bootstrap đúng thứ tự; 31 model/47 data tests theo baseline hiện tại hoặc số mới được giải thích |
| Chạy lại / restart | Dữ liệu còn, không tăng duplicate do replay; init không reset warehouse |
| Nguồn phát sinh thêm event | Incremental hợp lệ; không ép row counts bằng lần trước khi source thay đổi |
| Lỗi mạng, stale source hoặc dbt fail | Có logs/artifacts; DAG không báo success giả, dashboard không trả số 0 giả |
| Retry/Clear/backfill | Giữ transaction invariants và cập nhật đủ Silver/Gold theo phạm vi đã chọn |
| Cô lập | Không kết nối Neon của chủ repo, không đọc Secrets GitHub và không có DAG project khác |
| Bảo toàn cloud | Diff không thay lịch sync/credential/host Neon đang vận hành; CI vẫn pass |
| Tài nguyên | Có đo peak RAM/CPU, initial load time và disk; README ghi cấu hình máy dùng để đo |
| Nền tảng | Linux containers và Windows Docker Desktop được kiểm chứng hoặc ghi rõ nền tảng chưa test |

Fixture tests giữ vai trò kiểm thử xác định trong CI; smoke test nguồn public có thể chạy riêng. Không bắt CI mỗi PR phải kéo toàn bộ nguồn thật hoặc nhận credential Neon.

## 9. Dữ liệu bền, vận hành và bàn giao

Warehouse, metadata, logs và artifacts dùng volume riêng; `down`/restart thường ngày giữ dữ liệu. Hướng dẫn reset phải chỉ rõ volume nào bị xóa, không gộp thao tác reset vào quickstart. [Docker volumes](https://docs.docker.com/engine/storage/volumes/).

Trước release cần có: hướng dẫn backup/restore, dọn artifacts/container lỗi theo phạm vi, ghi nhận version image và dataset timestamp, cách cập nhật image, và changelog giới hạn metric. ClickHouse public vẫn là external dependency; Docker full không có nghĩa là bản live chạy offline.

Chỉ khi clone sạch chạy lại thành công, kiểm tra cô lập đạt và hướng dẫn khớp lệnh thật mới đánh dấu Docker full hoàn thành. Không đổi lịch Actions/Neon để nghiệm thu bản public.
