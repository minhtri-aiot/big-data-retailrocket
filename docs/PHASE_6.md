# Phase 6 — phục vụ đúng Graph artifact của Phase 5

FastAPI runtime mặc định chỉ chấp nhận algorithm `graph_session`; pointer còn trỏ ALS cũ làm readiness 503, không fallback về ALS. Tạo app với Settings trực tiếp vẫn hỗ trợ fixture lịch sử cho regression tests; đây không phải chính sách runtime mặc định.

- `POST /api/session/recommendations`: dùng events trong request, tối đa 100; Graph scorer đọc neighbors/transitions của run đã chọn. Chưa có events hoặc seed không có cạnh dùng popularity cùng bundle. Các request không chia sẻ context/cache phiên.
- `GET /api/recommendations/{visitorid}`: cùng Graph scorer, dùng tối đa 5 item gần nhất trong snapshot `user_context`; lọc toàn bộ lịch sử SQLite và lịch sử MongoDB/Redis nếu bật. Đây là ngữ cảnh lịch sử snapshot, không phải phiên trình duyệt hiện tại.
- `GET /api/metrics`: đúng metrics Graph được Phase 5 xuất. Trả cả model_version và metric_model_version. Không tự lấy số ở report cũ; không chấm lại từ lượt bấm.
- `GET /api/users`: mẫu ID có trong snapshot để demo, không phải danh sách user ALS đã xuất sẵn.
- `/health/ready`: kiểm tra bundle, checksum hai sidecar, schema SQLite, metadata version và data lineage. Sidecar sai/mất không được dùng để fallback bỏ qua seen. File JSON cùng run lỗi có thể giữ bản hợp lệ cũ chỉ khi sidecar của bản cũ vẫn đúng; đổi sang run lỗi thì 503.

Dashboard hiển thị Graph là mô hình chính, đủ P/R/NDCG/HitRate, baseline popularity và ALS khi thực sự đã chạy. K tương tác không đổi K cố định của báo cáo. Chỉ số là dự đoán item tương tác mới trong phiên, không phải accuracy phân loại hay xác suất mua.

Cold-start: user hoàn toàn mới nhưng đã xem seed có cạnh có thể nhận gợi ý Graph; chưa xem gì thì popularity; item mới chưa có cạnh cần tích lũy dữ liệu/retrain. Graph không tự có thông tin ngữ nghĩa từ ảnh/tên minh họa.

Airflow DAG `retailrocket_daily_retrain` chạy 02:00 Asia/Ho_Chi_Minh. Worker chỉ chấp nhận algorithm graph_session, thực hiện runner retrain từ nguồn clean đã commit, rebuild storage và Graph, đăng ký version, rồi đổi pointer. Sensor kiểm tra status và algorithm kết quả. Một job hoạt động tại một thời điểm, request ID dùng để idempotency. Retrain chạy lại một snapshot dữ liệu không đảm bảo điểm tăng. Cần phối hợp dừng live writer để snapshot clean nhất quán theo giới hạn hiện tại.

Các thao tác Xem/+Giỏ/Mua của UI vẫn chỉ cập nhật context trong tab. Chúng không được ghi Kafka/CSV hoặc tự train; không tạo thanh toán/đơn hàng thật. Kafka replay và ingest dữ liệu mới là luồng riêng. Airflow điều phối lịch retrain, không thực hiện trong HTTP request của khách.
