# Kiểm tra hệ thống và regression

**Dùng khi:** sửa code và muốn biết nên chạy test nào, hoặc cần phân biệt kiểm tra stack hiện tại với fixture CI. Nếu chỉ muốn xem dashboard, làm theo [Docker guide](docker.md).

## 1. Chọn đúng loại kiểm tra

| Nhu cầu | Cách chạy | Đích dữ liệu |
|---|---|---|
| Kiểm tra stack/dữ liệu hiện có | `verify_local_stack.py` | Warehouse hiện hành, không trigger ETL |
| Nghiệm thu một lượt ETL mới | Verifier thêm `--run-pipeline` | Airflow đọc ClickHouse, ghi warehouse local |
| Kiểm thử code/SQL bằng dữ liệu nhỏ | Fixture runner + Python tests | Chỉ `laplaptech_pipeline_test` riêng |
| Đọc bằng chứng trước đây | [Danh sách report ở mục 8](#8-báo-cáo-kiểm-chứng-theo-ngày) | Snapshot theo thời điểm ghi report |

Setup stack xem [Docker](docker.md); lệnh dbt và ý nghĩa artifacts xem [dbt](dbt.md). Chạy các lệnh sau từ **root repo**. Không suy ra trạng thái realtime từ report lịch sử.

## 2. Kiểm tra stack đã chạy

Cần Python 3.10+ và Docker CLI; verifier chỉ dùng thư viện chuẩn Python nên không cần cài requirements trên host:

```powershell
python scripts/verify_local_stack.py --output scratch/system-check.json
```

Verifier mặc định kiểm tra năm service nền healthy, DAG import errors, giới hạn RAM/CPU thực tế và pool dùng chung/schedule, target identity, dependency lock, raw → Gold reconciliation và HTTP Airflow. Khi dashboard đã được bật, thêm `--with-dashboard` để kiểm tra HTTP, database và AppTest của nó. Với `--run-pipeline`, verifier còn đối chiếu cgroup bốn task với policy. Probe/AppTest có thể tạo container/process kiểm tra; mặc định không trigger ingestion/build. Xem [resource audit](resources.md) khi chỉ cần kiểm tra tài nguyên.

Nếu chọn workspace platform khác lúc bootstrap, truyền cùng đường dẫn qua `--platform-dir`:

```powershell
python scripts/verify_local_stack.py --platform-dir '../airflow-platform' --output scratch/system-check.json
```

**Checkpoint:** exit code 0 và report có `status: success`. Nếu đang có queued/running run, đợi run hoàn tất rồi kiểm tra lại.

Để chạy một lượt pipeline mới thật sự, giữ DAG unpaused, đợi hết active run rồi:

```powershell
python scripts/verify_local_stack.py --run-pipeline --output scratch/system-check-live.json
```

Lệnh này trigger một run incremental với `full_refresh=false`, đợi bốn task success rồi xác nhận **31 model/47 data tests** trong artifacts của đúng run. Nó có đọc nguồn và ghi warehouse; không dùng để thử dữ liệu fixture. Khi timeout, run vẫn còn trong Airflow để kiểm tra, không bị verifier tự xóa.

## 3. Chuẩn bị Python và PostgreSQL test riêng

Dùng một terminal PowerShell riêng cho phiên test. Cần Docker Linux Engine và Python 3.13. Từ root repo:

```powershell
if (-not (Test-Path -LiteralPath '.venv/Scripts/python.exe')) {
    py -3.13 -m venv .venv
}
$python = '.\.venv\Scripts\python.exe'
& $python -m pip install -r requirements.txt
& $python -m pip check
```

Kiểm tra cổng **55435** chưa được project khác dùng, rồi tạo container test có tên theo phiên. Container này không dùng volume warehouse Airflow:

```powershell
$testContainer = 'laplaptech-test-' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ')
docker run --rm --name $testContainer -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=local_test_only -e POSTGRES_DB=laplaptech_pipeline_test -p 127.0.0.1:55435:5432 -d postgres:16
if ($LASTEXITCODE -ne 0) { throw 'Test PostgreSQL did not start; inspect the port/container error.' }
docker exec $testContainer pg_isready -U postgres -d laplaptech_pipeline_test
```

Đợi đến khi `pg_isready` trả `accepting connections`; nếu vừa khởi tạo còn báo chưa sẵn sàng, chạy lại dòng kiểm tra sau vài giây. Không xóa container/volume của project khác để giải phóng cổng.

## 4. Fixture full build, replay và Python tests

Đặt URL test trong terminal vừa tạo:

```powershell
$env:DEV_DATABASE_URL = 'postgresql+psycopg2://postgres:local_test_only@localhost:55435/laplaptech_pipeline_test'
& $python -m scripts.run_dev_pipeline --source fixture
```

Fixture runner không đọc ClickHouse: nó thay source client bằng dữ liệu xác định, nhưng dùng ingestion/loader và SQL dbt thật. Lượt chạy gồm ingestion → freshness → full dbt build → ingestion replay → incremental build → đối soát row counts và các ranh giới ngày/tháng/JSON/metric.

**Checkpoint:** runner thành công, in `DEV_REPORT: .../report.json`; report có `success`, `fixture_boundary_checks_pass`, `replay_row_counts_unchanged` đều `true`. Chỉ tiếp tục khi fixture Gold đã build thành công:

```powershell
$env:TEST_DATABASE_URL = $env:DEV_DATABASE_URL
$env:DASHBOARD_TEST_DATABASE_URL = $env:DEV_DATABASE_URL
& $python -m unittest discover -s tests -v
```

**Checkpoint:** không có failure/error. Report kiểm chứng ngày 20/10/2026 ghi **43/43 Python tests pass**, gồm target guards, PostgreSQL rollback/replay/concurrency, bootstrap coordination và dashboard AppTest. Số test có thể tăng khi code thay đổi; đọc tổng kết thực tế của lượt vừa chạy.

Không đặt `DEV_DATABASE_URL` thành warehouse active `laplaptech_pipeline_dev`: runner chặn trước kết nối/ghi. Fixture và `--reset-fixture` chỉ chấp nhận `laplaptech_pipeline_test`; cờ reset xóa/recreate bốn schema fixture nên chỉ dùng khi chủ động reset database test đó. Runner nguồn ClickHouse chỉ cho test hoặc `laplaptech_pipeline_dev_live` riêng, không phải đường cập nhật warehouse đang dùng.

Các URL test/dev-live phải là localhost, có username/password rõ, không dùng query override như `?host=...`/`?dbname=...`. Contract thực thi nằm trong [`run_dev_pipeline.py`](../../scripts/run_dev_pipeline.py).

## 5. Chạy nhóm test khi đang sửa code

Giữ cùng môi trường Python của mục 3:

```powershell
& $python -m unittest discover -s tests -p test_airflow_stage.py -v
& $python -m unittest discover -s tests -p test_dev_pipeline_guard.py -v
& $python -m unittest discover -s tests -p test_bootstrap.py -v
& $python -m unittest discover -s tests -p test_resource_policy.py -v
```

Guard tests không kết nối warehouse thật. Bootstrap tests dùng Docker giả trong thư mục temp; cần PowerShell và sẽ skip nếu máy không có PowerShell. Không có `TEST_DATABASE_URL`, PostgreSQL integration tests skip; không có fixture Gold/`DASHBOARD_TEST_DATABASE_URL`, hai AppTest fixture skip. Vì vậy chỉ chạy unit tests không thay thế bước full regression ở mục 4.

## 6. CI kiểm tra gì?

Workflow duy nhất [`validate.yml`](../../.github/workflows/validate.yml) chạy khi pull request hoặc push `develop`/`main`:

1. Cài Python 3.13 và requirements lock, kiểm tra dependency và compile Python.
2. Chạy unit/guard và integration tests trên PostgreSQL service test riêng.
3. dbt parse bằng cấu hình giả; chưa kết luận dữ liệu từ parse.
4. Fixture full build/replay, sau đó dashboard AppTest trên Gold fixture.
5. Upload report, manifest, run results và freshness artifacts đã sinh.

CI không cần ClickHouse/Neon secrets và không cập nhật warehouse local. Khi Docker/laptop tắt, CI không chạy ETL thay Airflow.

## 7. Dọn tài nguyên của phiên test

Sau khi đã giữ report cần thiết, dừng **đúng container test do phiên này tạo**:

```powershell
docker stop $testContainer
Remove-Item Env:DEV_DATABASE_URL, Env:TEST_DATABASE_URL, Env:DASHBOARD_TEST_DATABASE_URL -ErrorAction SilentlyContinue
```

`--rm` đã đặt lúc tạo container nên Docker tự xóa container test sau khi dừng. Warehouse/metadata/config/artifact volumes của Airflow giữ nguyên. Không dùng `docker system prune` hoặc `compose down -v` để dọn một phiên test.

Lỗi connection refused: kiểm tra readiness/cổng; `28P01`: nhập đúng credential test; guard chặn: sửa URL về database test thay vì bỏ guard; model/test lỗi: đọc lỗi đầu và `run_results.json`; AppTest thiếu Gold: chạy fixture runner thành công trước. Thêm chi tiết dbt trong [dbt guide](dbt.md).

## 8. Báo cáo kiểm chứng theo ngày

Repo giữ hai report tham khảo: [system check](../validation/system_check_2026-10-20.json) và [regression](../validation/regression_check_2026-10-20.json). Đây là ảnh chụp kiểm chứng theo ngày, **không phải trạng thái hiện tại**. Report thao tác máy cá nhân như migration/cutover hoặc restart Docker được giữ local, không thuộc tài liệu chạy của repo public.

[Functional check 29/10/2026](../validation/functional_check_2026-10-29.json) ghi 60 Python tests, fixture build/replay, 15 quan sát late-data và kiểm lịch 07:00/19:00. Report phân biệt hành vi được hỗ trợ với giới hạn đã tái hiện; không phải benchmark throughput hoặc chứng nhận mọi late event đều được bắt.

Giữ report lịch sử nguyên trạng; tạo report mới khi kiểm tra lại. Không suy ra đã diễn tập restore hoặc đã xóa Neon từ các report local.

## 9. Kiểm định lookback và late-arriving data

[assess_late_data.py](../../scripts/assess_late_data.py) dùng source fixture nhưng chạy loader và dbt/PostgreSQL thật. Script chỉ chấp nhận `DEV_DATABASE_URL` tới localhost + `laplaptech_pipeline_test`, qua guard của dev runner. Cờ bắt buộc `--reset-fixture` xóa/recreate bốn schema fixture; chỉ chạy trên DB test dùng riêng cho phiên, không dùng để đánh giá trực tiếp warehouse vận hành.

Chuẩn bị Python/DB test theo mục 3 và gán `DEV_DATABASE_URL` theo mục 4. Khi chạy CLI ngoài pool, cần cửa sổ không có writer cạnh tranh, dừng scheduler sau khi drain và ghi trạng thái để khôi phục. PostgreSQL test cần RAM/CPU cap và host còn đủ RAM; không suy RAM container thấp nghĩa là Windows còn nhiều bộ nhớ.

```powershell
$assessmentOutput = Join-Path (Get-Location).Path ('scratch/late-data-' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ'))
& $python -m scripts.assess_late_data --reset-fixture --output $assessmentOutput
```

Output phải là thư mục chưa tồn tại. Xem `report.json`, các `builds`/`cases` và artifacts dbt của từng lần build; log console đã che password test. Các case gồm: trong cửa sổ, đúng biên, ngoài cửa sổ, `[start,end)`, backfill + incremental/full rebuild, correction, replay, version cũ/xung đột và raw/Silver lệch lookback. Có thêm event client-time cũ một ngày nhưng server-received time mới.

**Ý nghĩa kết quả:** `supported` là hành vi đã đạt; `limitation_confirmed` là giới hạn cửa sổ đã tái hiện; `limitation_detected_by_test` là chênh lệch được dbt tests phát hiện. Một số build cố ý phải fail `assert_raw_to_silver`, rồi full-refresh phục hồi. Script exit 0 với `completed_with_known_limitations` chỉ xác nhận quan sát khớp đặc tả hiện tại, không có nghĩa mọi ca đều được xử lý tự động.

Lookback lấy từ **MAX timestamp bảng đích trừ 2 giờ**, không phải `now() - 2 giờ`. Đổi lịch thành 07:00/19:00 không tự gây bỏ toàn bộ event giữa hai lượt cách nhau 12 giờ. Rủi ro là row xuất hiện muộn nhưng mang server timestamp cũ hơn cutoff; client event-time cũ mà server timestamp mới vẫn được nhận.

Khi sửa recovery/lookback, cập nhật expected outcomes cùng tests, thay vì giữ ca lỗi như kết quả mong đợi mãi. Các số thời gian trên fixture nhỏ không đủ để chọn index, tăng RAM hoặc kết luận tốc độ trên dataset thật.
