# RetailRocket — hệ thống gợi ý Graph thống nhất Phase 5–6

Mô hình chính duy nhất là **Graph-based Session Recommendation**: Item–Item có điều kiện + Session Transition. Apache Spark xây dựng cả hai đồ thị. FastAPI dùng artifact do Phase 5 xuất; Dashboard đọc đúng metrics của artifact đó. ALS chỉ phục vụ đối chứng, không được tự động chọn làm mô hình phục vụ.

**Luồng:** RetailRocket → Kafka → Spark → Delta/HDFS + MongoDB/Redis → Graph Training (Phase 5) → FastAPI → Dashboard (Phase 6). Airflow gọi worker huấn luyện lại Graph lúc **02:00 Asia/Ho_Chi_Minh** mỗi ngày.

## Bắt đầu với bản demo đã train

Tải [bản Graph đã kiểm tra trên Windows/Docker](https://github.com/bluejames1106/big-data-retailrocket/releases/tag/graph-unified-v1).
Kết quả kiểm tra ngày 25/10/2026 nằm tại [LOCAL_REVIEW_20261025.md](docs/LOCAL_REVIEW_20261025.md).

Cài Docker Desktop (Linux containers), giải nén mã nguồn vào thư mục mới, mở PowerShell tại thư mục chứa README và chạy:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run_graph_demo.ps1
```

Mở http://localhost:8000/ và http://localhost:8000/docs. Script cài snapshot Graph đi kèm rồi chạy API. Chạy snapshot không cần Kafka, HDFS hoặc Airflow. Đây là kiểm tra luồng phục vụ, không đại diện cho việc chạy lại toàn bộ hạ tầng Big Data. Lần build đầu cần Internet. Xem [hướng dẫn đầy đủ](docs/GRAPH_RUNBOOK.md).

## Train Graph

Python 3.11/3.12, Java 17 và các dependencies:

```bash
python -m pip install -r requirements-dev.txt
python -m pipeline.graph_training --input data/sample/events_10000.csv --run-id graph-sample-01 --compare-als --activate
python -m uvicorn service.app:app --host 127.0.0.1 --port 8000
python -m scripts.verify_graph --run-id graph-sample-01
```

Để train toàn nguồn, đặt `events.csv` tại `data/raw/events.csv`, thay `--input` ở lệnh trên và dùng run ID mới. CSV đầy đủ lấy từ nguồn RetailRocket/Kaggle hoặc Release cũ `storefront-v2`; nó không được thay bằng dữ liệu tổng hợp. `--compare-als` chỉ chấm một baseline ALS trên cùng cohort test, không thay đổi họ mô hình. `--full-grid` chọn tỷ lệ chuyển tiếp bằng hai fold validation; mặc định giữ chính sách Graph 75/25 đã chọn.

Để đi qua **Kafka → Spark → Storage → Graph**, dùng:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run_demo.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/start_scheduler.ps1
```

DAG retrain yêu cầu một run dữ liệu tích hợp còn giữ thư mục `clean/`; snapshot demo chỉ phục vụ API không phải nguồn retrain Kafka. Không dùng snapshot demo để khẳng định scheduler đã chạy.

## Mô hình và đánh giá

- Item–Item: đồng xuất hiện theo visitor nhị phân, giữ tối đa 20 item phân biệt gần nhất/visitor; điểm conditional có shrinkage support.
- Transition: cạnh từ nhóm timestamp trước đến item mới của nhóm timestamp kế tiếp trong phiên gap 30 phút; không tạo cạnh giữa item đồng timestamp.
- Tối đa 100 hàng xóm/nhánh/seed, 5 seed gần nhất, decay vị trí 0.8; trọng số view/cart/purchase **1/1/1**. Fusion điểm chuẩn hóa: **75% Item–Item + 25% Transition**, căn bậc hai độ mạnh.
- Phiên chưa có ngữ cảnh hoặc seed chưa có cạnh: fallback phổ biến của cùng model, loại item đã tương tác. Graph không tự giải quyết item hoàn toàn mới chưa có cạnh.
- Đánh giá: mọi item mới còn lại trong cùng phiên, K=10; Precision, Recall, NDCG, **HitRate** macro/query. Cold user/item không bị loại để tăng điểm. Đây là dự đoán tương tác, không phải xác suất mua.
- Temporal folds 50→70%, 70→85%; test sau 85%. Metrics dùng graph fit trước cutoff; graph phục vụ được refit toàn dữ liệu sau khi metrics đã cố định.

Xem [phân tích ALS và Graph](docs/ALS_VS_GRAPH.md), [kết quả đo của bản này](docs/GRAPH_RESULTS.md), [phạm vi kiểm thử](docs/GRAPH_VALIDATION.md). Các báo cáo cũ dưới `reports/` và tài liệu thí nghiệm còn được giữ để truy vết; chúng không phải số liệu của bản hiện tại.

## Tài liệu và mã nguồn

| Đường dẫn | Vai trò |
|---|---|
| `pipeline/graph_training.py` | Train/evaluate/export Graph bằng Spark; CLI độc lập và entry point Delta |
| `pipeline/session_model.py`, `session_transition.py` | Scorer dùng chung giữa offline/API |
| `pipeline/runner.py`, `full_train.py` | Pipeline tích hợp và train toàn nguồn qua storage |
| `service/` | FastAPI + Dashboard; xác thực phiên bản và checksum sidecars |
| `orchestration/dags/`, `pipeline/job_server.py` | Lịch retrain và worker chỉ chấp nhận Graph |
| `demo/graph_snapshot/` | Snapshot Graph có thể cài vào outputs, SQLite nén gzip |
| `tests/`, `reports/verification/graph_unified/` | Kiểm thử và bằng chứng của bản sửa |

Hướng dẫn đưa lên repo đang có: [GITHUB_UPDATE.md](GITHUB_UPDATE.md). Bản mã nguồn này không chứa `.git`, `.env` riêng, Docker volume hoặc Docker image.

Dữ liệu gốc là [Retailrocket recommender system dataset](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset),
giấy phép [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/).
Snapshot và bản ghi đánh giá được tạo từ bộ dữ liệu nghiên cứu công khai có ID đã băm;
tên, ảnh, giá trên cửa hàng là minh họa.
