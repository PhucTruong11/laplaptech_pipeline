# Vận hành Cosmos cho LapLapTech

Cosmos 1.15.1 dựng dependency từ manifest của dbt. Scheduler gửi từng model tới runtime Docker qua `BudgetDockerOperator`. dbt và adapter PostgreSQL nằm trong runtime LapLapTech, còn SQL chạy trong warehouse.

```text
ingest → freshness → transform (31 model tasks) → dbt_tests (47 tests)
       → collect_dbt_results → reconcile → validated Gold Asset
```

DAG chính giữ ID `laplaptech_pipeline`, một active run, một active task và lịch 07:00 / 19:00 giờ Việt Nam. Các model và báo cáo dùng ngân sách stage `build`. Các data test chạy sau toàn bộ graph, nên model đã materialize có thể tồn tại dù test về sau thất bại. Collector và reconcile đều phải đạt trước khi phát Gold Asset. Dashboard hiện có thể đọc các bảng giữa chừng vì Gold vẫn chưa publish trong một transaction chung.

## 1. Chuẩn bị và phát hành runtime

Với cài mới, [bootstrap Docker](../guides/docker.md) build image cả Airflow và LapLapTech, xuất manifest và chờ DAG processor nhận đúng release. Với workspace hiện có ở chế độ Cosmos, chạy lại `Start-LocalStack.ps1` sau khi đọc [runbook Docker](../guides/docker.md). Script pause/drain trước cập nhật và đợi serialized DAG sử dụng SHA mới trước khi khôi phục lịch.

Muốn tự làm đúng phần phát hành runtime, từ root repo, trong cửa sổ bảo trì đã pause/drain:

```powershell
docker build -f infra/laplaptech/runtime.Dockerfile -t laplaptech-runtime:local-v1 .
if ($LASTEXITCODE -ne 0) { throw 'Runtime build failed.' }
.\scripts\Publish-CosmosRelease.ps1
```

Dockerfile chạy `dbt parse` trong chính image, với profile giả cho parse. Script lấy manifest ra từ image, tính SHA256, gắn tag `laplaptech-runtime:cosmos-<32 ký tự SHA>` rồi mới thay file manifest. Runtime mỗi task xác minh SHA trước khi đọc danh sách model/test. Giữ image release cũ trong khi còn run hoặc retry phụ thuộc vào nó.

Manifest nằm ở `infra/laplaptech/dags/.cosmos/manifest.json`. Ba service Airflow đọc file qua DAG bind mount hiện có. Thư mục `.cosmos` nằm ngoài Git và Docker build context. Không cần khai báo overlay Compose thứ ba.

## 2. Đọc graph và retry

- `transform.<model>` gọi `dbt run` với selector FQN của đúng một model. Manifest giữ model ID; runtime đối chiếu `run_results.json` phải có đúng ID và trạng thái `success`.
- `dbt_tests` gọi `dbt test` sau tất cả model. Collector kiểm toàn bộ test ID của release, từ chối artifact khác release/run hoặc attempt mới nhất thất bại.
- `reconcile` kiểm inventory và số liệu Raw → Gold; chỉ task cuối này phát Gold Asset cho DAG chính.
- Lỗi tạm thời ở model có một lượt retry tự động. Nếu tự Clear sau khi sửa lỗi, chọn model lỗi và downstream bị ảnh hưởng, gồm tests, collector và reconcile. Khi có thay đổi SQL hoặc manifest, phát hành release mới rồi chạy run mới. Giữ SQL và trạng thái upstream đúng phiên bản khi retry.

Mỗi model dùng thư mục `/artifacts/<run-hash>/cosmos/<model-id>/attempt-...`. Global test dùng `cosmos/tests/`. Summary là `/artifacts/<run-hash>/cosmos-summary.json`, có số model/test và thông tin task attempts. Verifier còn đối chiếu attempt trong Airflow và cgroup với resource policy:

```powershell
python scripts/verify_local_stack.py --run-pipeline --with-dashboard --output scratch/cosmos-verification.json
```

Tham số `--run-pipeline` thực hiện một lượt thật; bỏ tham số đó nếu chỉ kiểm stack đang chạy. Số lượng checkpoint hiện tại là **36 Airflow task, 31 dbt model, 47 data test**. Các lượt trước chuyển sang Cosmos vẫn có artifact `build` theo định dạng cũ.

## 3. Trial và quay lại luồng cũ

File mode local `infra/laplaptech/dags/.cosmos/mode` nhận `cosmos`, `trial` hoặc `legacy`. Trên workspace chưa tạo file mode, DAG dùng chế độ `legacy`. Đổi mode trong cửa sổ đã drain; chờ processor parse đúng graph trước khi unpause hoặc trigger. Có thể dùng PowerShell từ root repo:

```powershell
$cosmosMode = Join-Path (Get-Location).Path 'infra/laplaptech/dags/.cosmos/mode'
[IO.File]::WriteAllText("$cosmosMode.next", 'trial')
[IO.File]::Move("$cosmosMode.next", $cosmosMode, $true)
```

`trial` giữ DAG chính ở luồng cũ và đăng ký `laplaptech_cosmos_trial` ở chế độ manual. Trial ghi cùng warehouse, nên chỉ trigger khi DAG chính đã paused và không còn writer cạnh tranh. Trial không phát Raw/Gold Assets. Sau khi trial dừng và đối soát đạt, pause trial rồi đổi mode thành `cosmos` để DAG chính nhận graph mới. Khi cần quay lại, dùng mode `legacy` theo cùng trình tự pause/drain và xác nhận Graph. Quay lại DAG cũ không đảo ngược bảng đã materialize. Nếu lần chạy lỗi sau khi ghi Gold, dùng lượt kiểm chứng hoặc recovery để đồng bộ dữ liệu.

[Airflow guide](../guides/airflow.md) có lệnh thao tác thường ngày. [Tài liệu Cosmos về manifest](https://astronomer.github.io/astronomer-cosmos/guides/translate_dbt_to_airflow/parsing-methods.html) giải thích cách Cosmos chuyển dependency dbt thành graph.
