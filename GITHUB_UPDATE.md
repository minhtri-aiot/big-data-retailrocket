# Thay mã nguồn repo GitHub bằng bản Graph thống nhất

ZIP chứa toàn bộ mã nguồn đã chỉnh cùng snapshot demo và báo cáo. Không đính kèm thư mục .git; không tự push hoặc thay đổi repo từ xa trong lần giao này.

1. Giải nén ZIP vào thư mục mới. Giữ bản cũ và outputs cũ riêng để có thể đối chiếu.
2. Nếu bạn đã clone repo: chép toàn bộ nội dung thư mục `big-data-retailrocket/` từ ZIP vào thư mục clone đang có, gồm cả .gitignore, .dockerignore và .gitattributes. Giữ nguyên .git của clone. Không trộn runtime outputs cũ; API mặc định sẽ từ chối pointer còn trỏ ALS.
3. Mở terminal tại clone, kiểm tra diff rồi commit/push:

```bash
git status
git diff --stat
git add -A
git commit -m "Unify Phase 5 and Phase 6 on Spark session Graph recommendation"
git push origin main
```

4. Sau push, README phải ghi Graph thống nhất; demo/graph_snapshot và data/sample phải có trên repo. Raw CSV, outputs và các SQLite sinh trong runtime đã nằm trong .gitignore. Các SQLite nén `.sqlite.gz` của demo được giữ để cài snapshot. Giữ tệp lớn hơn giới hạn GitHub trong Release/Git LFS; không commit raw/output bằng force. Gói có snapshot model lớn, nên dùng git push như trên; nếu chỉ tải lên một ZIP trên giao diện web thì GitHub không hiển thị mã nguồn bên trong để đọc. Có thể phát hành snapshot riêng ở Release/Git LFS khi muốn giảm dung lượng repo.
5. Chạy scripts/run_graph_demo.ps1 trên máy mới để demo snapshot. Muốn huấn luyện toàn nguồn, lấy events.csv từ nguồn đã ghi và dùng hướng dẫn GRAPH_RUNBOOK.md. Nếu chỉ cài snapshot, không tuyên bố đã chạy Kafka/Spark/HDFS/Airflow trên máy đó.

Release storefront-v2 cũ vẫn là bản lịch sử. Khi phát hành bản mới, đặt tên/tag riêng (ví dụ graph-unified-v1) để người dùng không tải nhầm mô hình cũ. Không ghi đè checksum hoặc số liệu của Release lịch sử.
