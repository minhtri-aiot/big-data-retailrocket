# Phạm vi kiểm thử bản Graph thống nhất

**Cập nhật 25/10/2026:** bản ZIP đã được kiểm tra thêm trên máy Windows với Docker
và trình duyệt thật. Xem [báo cáo mới](LOCAL_REVIEW_20261025.md). Bảng dưới giữ
phạm vi kiểm tra của lần bàn giao ban đầu; các dòng Docker/trình duyệt “chưa
chạy” ở đây không mô tả lần kiểm tra mới.

| Hạng mục | Kết quả thực tế |
|---|---|
| Spark Graph train/evaluate/export toàn RetailRocket | Đã chạy: 2.755.641 sự kiện sạch, hai validation fold, test5.000 query, refit toàn nguồn |
| ALS Spark MLlib đối chứng | Đã chạy trên cùng cutoff/cohort/labels/K/seen; chỉ baseline, không phục vụ |
| Unit/regression chạy cách ly | 145 test:143 đạt,2 skip; cả17 module exit0. Bật Graph và ML Spark opt-in |
| Spark relation/session tests | Đạt: support/shrinkage, đồng timestamp, gap30phút, target lặp, context quá khứ, nhiều label |
| API với artifact full | Đạt: readiness, Graph identity/checksum, cùng scorer offline, phiên trống/seed lạ, lọc seen, GET/POST Graph |
| Tự tính lại metrics | Đạt trên5.000 dòng test_rankings.jsonl, sai số<1e-12 |
| Dashboard JS/DOM + HTTP thật | Đạt bằng jsdom:4metric cards, model/version Graph, Xem/+Giỏ/Mua mô phỏng, reset, không lỗi JS |
| Cài snapshot mới từ gzip | Đạt trong output root mới; checksum/version khớp |
| Delta Silver cục bộ → trainer chính | Đạt trên fixture100sự kiện, pin version0 và validate artifact; Delta jars3.3.3 nạp sẵn từ Maven chính thức |
| Airflow DAG/worker | Contract tests đạt: lịch02:00 VN, submit→sensor, request Graph, worker/result version, từ chối thuật toán khác. Import DAG dùng stub và HTTPmock |
| Docker build/Compose, Kafka/HDFS, MongoDB/Redis thật | Chưa chạy trong môi trường giao việc; vẫn cần verify_e2e trên máy có Docker |
| Airflow scheduler thật | Chưa chạy; không coi contract tests là một DAG run thật |
| Trình duyệt thật, layout/mobile | Chưa kiểm chứng; Chromium download không khả dụng. DOM test không xác nhận bố cục/font/responsive |

Đã thử chạy Delta test qua Maven resolver mặc định, nhưng Java không resolve được host Maven. Đã xác minh riêng đường Delta→Graph bằng JAR chính thức nạp sẵn. Không xem đó là kiểm tra khả năng tải Maven khi build Docker trên máy khác. Log lỗi dependency được giữ cùng bằng chứng.

Lượt pytest đầu không đặt SPARK_LOCAL_IP gặp lỗi bind driver. Khi đặt 127.0.0.1 và chạy test cách ly như scripts/test_all.py, tất cả17 module hoàn tất. Test NoSQL và Delta tổng quát nằm trong2skip của bộ regression; đường Delta→Graph mới được chấm riêng như bảng. Không tuyên bố các test bị skip đã chạy.

Bằng chứng ở `reports/verification/graph_unified/`: summary.json, isolated-tests.log, graph_http_verification.json, dashboard_dom.json, delta_graph.json, source profile, split/config và model metrics. Snapshot chứa test_rankings.jsonl để tự tính lại. Sau bước chốt, bổ sung kiểm tra cấu hình Graph sai kiểu: lượt targeted cuối10đạt/3Spark test skip (đã chạy ở bộ cách ly trước). Log final-graph-contracts.log ghi riêng, không cộng lặp vào145test regression.

Các log/report2026-10-22 hoặc storefront-v2 là lịch sử, không dùng thay bằng chứng bản mới.

## Lệnh tái tạo

```bash
SPARK_LOCAL_IP=127.0.0.1 BIGDATA_RUN_GRAPH_TESTS=1 BIGDATA_RUN_SPARK_ML_TESTS=1 python -m scripts.test_all
python -m scripts.verify_graph --run-id YOUR_RUN --base-url http://127.0.0.1:8000
```

PowerShell đặt các biến bằng `$env:SPARK_LOCAL_IP='127.0.0.1'` và tương tự cho hai biến test, rồi chạy Python. Dashboard DOM test tùy chọn: cài jsdom26.1.0 và chạy scripts/verify_dashboard.cjs với API đang mở. Các công cụ kiểm thử không phải dependency phục vụ mô hình.
