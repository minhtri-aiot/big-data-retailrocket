# Train, chạy, kiểm thử và demo

## 1. Chuẩn bị

Giải nén vào thư mục mới; không trộn outputs/model của Release cũ. Docker Desktop dùng Linux containers, hoặc Python 3.11/3.12 + Java17 để chạy Graph cục bộ. Dự án dùng PySpark3.5.6. Cài dependencies từ requirements-dev.txt trong venv nếu chạy Python trực tiếp. Docker image cài runtime riêng trong Python3.11; lần build đầu cần Internet.

Bản nguồn có `data/sample/events_10000.csv` là dữ liệu thật, chỉ dùng smoke/demo nhỏ. CSV đầy đủ không đưa lên git; tải từ RetailRocket/Kaggle hoặc Release `storefront-v2` của repo gốc, đặt tại `data/raw/events.csv`. SHA/source đi cùng `data/metadata`. Không tạo dữ liệu tổng hợp để thay thế benchmark.

## 2. Chạy snapshot đi kèm

Windows PowerShell:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run_graph_demo.ps1
```

Script build image, cài demo/graph_snapshot vào outputs, xác thực Graph và khởi động compose.graph-demo.yaml. Nếu outputs đã có Graph hợp lệ, giữ model đó; nếu pointer trỏ ALS cũ hoặc bị hỏng, báo lỗi để bạn giữ/di chuyển thư mục cũ trước khi chạy. Không tự ghi đè run.

Python trực tiếp:

```bash
python -m scripts.setup_graph_demo
python -m uvicorn service.app:app --host 127.0.0.1 --port 8000
```

Mở localhost:8000. Snapshot là artifact do bản trainer mới sinh, không phải model storefront-v2 cũ. Tệp SQLite gzip được giải nén rồi kiểm tra checksum/version khi cài. Metadata nguồn và phạm vi đo nằm trong bundle/metrics.

## 3. Train và kích hoạt model mới

```bash
python -m pipeline.graph_training --input data/sample/events_10000.csv --run-id graph-sample-01 --compare-als --activate
python -m pipeline.graph_training --input data/raw/events.csv --run-id graph-full-01 --query-cap 5000 --compare-als --activate
```

Mỗi lệnh là một lựa chọn, không bắt buộc chạy cả hai. Đặt bộ nhớ driver 3–4GB qua PYSPARK_SUBMIT_ARGS cho full CSV; máy có ít nhất 8GB RAM và chỗ disk cho bốn SQLite graph trung gian. Ví dụ PowerShell:

```powershell
$env:PYSPARK_SUBMIT_ARGS='--driver-memory 3g --conf spark.driver.maxResultSize=512m pyspark-shell'
python -m pipeline.graph_training --input data/raw/events.csv --run-id graph-full-01 --compare-als --activate
```

`--full-grid` tune tỷ lệ transition trên validation; không truyền thì cố định75/25. `--compare-als` thêm baseline, không xuất ALS làm primary. `--format parquet` cho parquet sạch hoặc `--format delta` cho Delta (cần Maven JAR tải được); CLI pin phiên bản Delta trước khi đọc. ID run không trùng và không được chứa đường dẫn.

`--activate` xác thực artifact rồi đổi outputs/latest_run.json. API không cần restart khi pointer chuyển sang run mới hợp lệ. Nếu không có flag, model chỉ được xuất, chưa đổi model phục vụ.

## 4. Chạy luồng Big Data đầy đủ

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run_demo.ps1
```

Lệnh này khởi động Kafka/HDFS/MongoDB/Redis, replay mẫu thật, xử lý Spark/Delta, train Graph, kiểm tra ACID, publish registry, kích hoạt API và chạy verify_e2e. Chưa chạy toàn nguồn Kafka thì không gọi nó là benchmark toàn nguồn streaming. Full CSV phải sort theo timestamp trước Kafka replay bằng scripts/sort_events.py. Đường offline full qua storage sẵn có:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run_full_training.ps1 -RunId graph-storage-full-01
```

Lệnh trên đọc toàn CSV batch, build HDFS/Delta và Graph; không đi qua Kafka replay. Cả hai đường đều gọi cùng trainer/scorer Graph.

## 5. Airflow

Sau khi có run dữ liệu tích hợp còn clean/:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/start_scheduler.ps1
```

UI Airflow: localhost:8088, local demo admin/demo-admin theo script cũ. DAG retailrocket_daily_retrain: 02:00 giờ Việt Nam, catchup=False, max_active_runs=1. Trigger thủ công trong UI để kiểm tra riêng. Worker chỉ chấp nhận graph_session, sao chép clean từ canonical source run, rebuild storage/graph, publish registry, đổi pointer sau thành công; sensor xác nhận algorithm/version. Kiểm tra outputs/jobs/*.json, run_report, registry và version API trước/sau. Snapshot serving đi kèm không chứa clean/ của pipeline, vì vậy phải chạy initial integrated pipeline trước lịch retrain. Phối hợp dừng live writer trước snapshot/retrain, không chạy hai writer chung output.

## 6. Kiểm thử

```bash
python -m pytest tests -q
python -m scripts.verify_graph --run-id YOUR_RUN --base-url http://127.0.0.1:8000
```

Real Spark Graph test trên PowerShell:

```powershell
$env:BIGDATA_RUN_GRAPH_TESTS='1'
python -m pytest tests/test_graph_pipeline.py -q
```

Các regression ALS Spark cũ là opt-in BIGDATA_RUN_SPARK_ML_TESTS=1. Delta/NoSQL tests cần đúng hạ tầng; xem biến bật ở tests. verify_graph kiểm checksum, identity, HTTP cold-start, parity scorer và tự tính lại metric từ JSONL; không kiểm Kafka/HDFS. verify_e2e chạy trong app container với đủ hạ tầng. DAG contract tests dùng stub import và mock HTTP; không được gọi là chạy scheduler thật. Xem GRAPH_VALIDATION.md để biết những gì thực sự đã chạy trong lần sửa.

## 7. Kịch bản demo 3–5 phút

1. Mở Dashboard, xem số event/user/item, algorithm Graph, model_version/metric_model_version và bốn chỉ số.
2. Phiên trống → gợi ý phổ biến. Xem một sản phẩm có cạnh → danh sách Graph thay đổi; +Giỏ/Mua mô phỏng thêm context, item đã tương tác bị loại. Dùng seed có cạnh được ghi ở báo cáo HTTP của snapshot.
3. Thử Item ID chưa có trong training → fallback nếu không có seed nào có cạnh; không nói Graph biết mọi item mới.
4. Mở bảng đánh giá: label là item tương tác mới còn lại trong phiên, HitRate khác Recall. ALS chỉ là đối chứng nếu đã chạy, không tự đổi mô hình.
5. Mở /api/metrics đối chiếu version với bundle. Phần Airflow chỉ trình bày lịch/worker nếu chưa trực tiếp chạy scheduler trên máy demo.
6. Nói rõ click mô phỏng không persist Kafka, không online-train và không tạo đơn hàng. Chất lượng đo được không phải tăng CTR/doanh thu thật.
