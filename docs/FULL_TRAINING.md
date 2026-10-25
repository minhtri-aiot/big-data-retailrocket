> **Cập nhật Graph thống nhất:** nội dung bên dưới là hướng dẫn/lịch sử trước khi thống nhất Phase 5–6. Dùng [Runbook hiện hành](GRAPH_RUNBOOK.md) và [kiểm thử bản mới](GRAPH_VALIDATION.md). Không dùng số liệu cũ làm kết quả bản mới.

# Huấn luyện full dữ liệu và cách hiểu metric

Full data là đọc đủ 2.756.101 dòng events.csv, validate rồi dedup trước huấn luyện.
Số event sạch có thể thấp hơn số dòng raw vì bản ghi trùng. Không giới hạn số
event train. Mỗi fold vẫn chỉ học phần quá khứ của toàn nguồn; chỉ model serving
cuối cùng học tất cả event sạch sau khi đánh giá đã đóng băng.

## Chạy lại

Mở Docker Desktop Linux engine, mở PowerShell trong thư mục có compose.yaml:

```powershell
.\scripts\run_full_training.ps1
```

Máy đang chạy có 16 GB RAM, Docker khoảng 7,4 GB. Spark local[4], heap 3 GB,
6 user/item block ALS. Đây là nhánh OFFLINE: CSV -> kiểm tra/quarantine/dedup ->
Delta/HDFS -> temporal ALS -> model registry -> API. Không tuyên bố đã replay
toàn lịch sử qua Kafka hay đã đẩy 2,76 triệu fact/session vào MongoDB/Redis.
Mongo/Redis giữ registry; SQLite giữ seen history đầy đủ cho mọi user quan sát.

## Các thay đổi mô hình

- Giữ feedback view/cart/purchase=1/3/5; dùng log1p của tổng trọng số để giảm
  việc xem lặp một sản phẩm lấn át các tương tác còn lại.
- Rank 16, hai regParam 0.05/0.1, alpha 20; chọn bằng hai fold thời gian.
- 4 vòng lặp cho validation; 8 vòng cho evaluation và model serving cuối.
- Cold user/item vẫn được tính miss trong metric chính. Có thêm diagnostic
  warm-user riêng, không thay thế hoặc trộn vào điểm chính.
- Chia thời gian TRƯỚC aggregate. Holdout >85% không dùng chọn tham số.

## Giới hạn tính toán được ghi rõ

Không tạo gợi ý trước cho mọi cặp 1,4 triệu user x hàng trăm nghìn item.
Đánh giá chọn tối đa 1.000 user theo hash ID+seed 42 từ population eligible;
quy tắc cố định trước khi xem kết quả, giữ cả cold user. Mỗi user được so
với toàn catalog train, không dùng negative sampling để làm đẹp điểm.
Báo cáo ghi population_eligible_users, eligible_users và evaluation_sampling.

Model vẫn học đầy đủ user/item trong từng khoảng thời gian. JSON/Parquet gợi ý
được tính sẵn cho tối đa 1.000 user hoạt động nhiều nhất, chọn theo training
serving đã quan sát. User ngoài phạm vi nhận popularity fallback có nhãn và
vẫn lọc toàn bộ seen qua SQLite. Không gọi phạm vi xuất gợi ý là train sample.

## Tại sao demo 10.000 dòng có 0/0/0?

Trong demo cũ, 871/912 user test chưa có trong train (95,5% cold). Ba metric
bằng 0 nghĩa không có hit trong top-10 của cohort được chấm. Chạy ít dữ liệu
không phải lúc nào cũng cho 0; chạy full cũng không bảo đảm mô hình tốt.
Điểm full và điểm demo dùng cùng định nghĩa novel-item ranking và chia thời
gian, nhưng khác quãng thời gian dữ liệu, cohort và cấu hình; không suy ra
chỉ riêng một thay đổi hyperparameter gây toàn bộ mức cải thiện.

Kết quả thực tế nằm trong outputs/runs/<run>/metrics/model_metrics.json,
run_report.json, kpis.json; log ở outputs/evidence/full-training.log.
Không điền số điểm dự kiến thay cho kết quả đã chạy.

## Lưu trữ Docker của máy demo

Docker WSL data được chuyển sang D:\BIGDATA\DockerData qua CustomWslDistroDir; toàn bộ image và volume được giữ lại. Spark scratch nằm tại /tmp/full_spark_scratch trong đĩa Linux trên D. Không cần thay cấu hình Docker của máy khác nếu ổ lưu Docker còn đủ dung lượng.
