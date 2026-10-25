> **Cập nhật Graph thống nhất:** nội dung bên dưới là hướng dẫn/lịch sử trước khi thống nhất Phase 5–6. Dùng [Runbook hiện hành](docs/GRAPH_RUNBOOK.md) và [kiểm thử bản mới](docs/GRAPH_VALIDATION.md). Không dùng số liệu cũ làm kết quả bản mới.

# Demo hiện tại: Retail Signal Storefront

Dự án làm việc: `D:\BIGDATA\big_data_update_2`. Bản giao diện cửa hàng được xuất riêng thành `D:\BIGDATA\big_data_update_2_storefront_v2.zip`. File gốc `big_data_update_2.zip`, ZIP cửa hàng trước và bản dashboard trước được giữ nguyên.

## Chạy demo

Cần Docker Desktop đang hoạt động. Mở PowerShell trong thư mục vừa giải nén:

```powershell
cd D:\BIGDATA\big_data_update_2
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\run_local_update2_demo.ps1
```

Mở **http://localhost:8000/**. Lần đầu sau khi giải nén bản mới cần build image bằng lệnh trên. Những lần sau, nếu image đã đúng phiên bản, thêm `-SkipBuild` để mở nhanh hơn. Nếu cổng 8000 đang bị chiếm, thêm `-Port 8002` và mở cổng 8002.

Launcher dùng image `bigdata-update2:local`, project Docker `retailrocket-update2` và `compose.local-update2.yaml`. Mô hình đang chọn là `strength-20261022-v1`. Không cần HDFS, Kafka hoặc Airflow để phục vụ snapshot có sẵn.

## Trải nghiệm cửa hàng

- Có tìm kiếm không dấu theo tên minh họa hoặc ID, **19 danh mục cụ thể và nút Tất cả**, chi tiết sản phẩm và giỏ trong tab. Các nhóm gồm công nghệ, thời trang, sách, văn phòng phẩm và đồ dùng; mọi nhóm đều có sản phẩm.
- Danh mục khám phá gồm **161 ID thật trong `events.csv`**, bao phủ **32 loại hình sản phẩm minh họa**. Đây là tập dùng để trình diễn; không phải danh mục đầy đủ của toàn bộ dữ liệu.
- Tên, ảnh SVG, danh mục và **giá minh họa** được gán cố định theo ID. ID gốc vẫn hiển thị trên thẻ và trong chi tiết. Những thông tin này không phải metadata thật của RetailRocket và không tham gia mô hình.
- Danh sách **Dành cho bạn** giữ nguyên thứ tự API trả về; tìm kiếm hoặc danh mục của khu vực khám phá không sắp xếp lại kết quả gợi ý.
- **Mua ngay nằm ngay trên mỗi thẻ sản phẩm**, ở cả khu vực Dành cho bạn và Khám phá sản phẩm. Bấm trực tiếp ghi một `transaction` mô phỏng rồi cập nhật gợi ý; không tự thêm lượt xem, thêm giỏ hoặc tạo đơn hàng. Chi tiết cũng có nút Mua ngay · mô phỏng. Nếu yêu cầu cập nhật lỗi, giao diện báo rõ thay vì báo cập nhật thành công.
- Mở chi tiết tạo một lượt xem mô phỏng khi model sẵn sàng; thêm giỏ ghi một lần `addtocart`. Thao tác gửi tương tác được khóa trong lúc API đang xử lý.
- Đổi số lượng, xóa món hoặc xóa giỏ chỉ cập nhật giỏ mô phỏng. Không có thanh toán, đơn hàng hay doanh thu thực. Phiên mới xóa cả ngữ cảnh và giỏ.
- Đánh giá, thông tin dữ liệu, gợi ý theo lịch sử người dùng và vận hành nằm trong các mục thu gọn. Tài liệu API vẫn tại **http://localhost:8000/docs**.

Xem hướng dẫn trình diễn và phạm vi của từng thao tác trong `docs/STOREFRONT_DEMO.md`. Bản mô tả dashboard trước còn tại `docs/DASHBOARD_STUDIO.md`.

## Đọc chỉ số cho đúng

Phép chấm chính dự đoán **các món mới còn lại trong cùng phiên**, trên 5.000 lượt dự đoán:

| Mô hình | Precision@10 | Recall@10 | NDCG@10 |
| --- | ---: | ---: | ---: |
| Đang phục vụ | 4,148% | 28,2803% | 19,7041% |
| Bản trước, cùng phép chấm | 4,116% | 28,0809% | 19,3253% |
| Phổ biến, cùng phép chấm | 0,172% | 0,9148% | 0,4982% |

Dashboard làm tròn để dễ đọc. Chọn hiển thị 5 hoặc 20 sản phẩm không thay đổi báo cáo đánh giá cố định ở Top 10. Chỉ số không được tính từ thao tác bấm thử.

Phép chấm phụ chỉ **một món mới tiếp theo** có P@10 3,022%, R@10 30,22%, NDCG@10 19,5479%. Phép chấm lịch sử user là mục tiêu khác nữa; không so trực tiếp các mức phần trăm giữa các phép chấm.

Chỉ NDCG có khoảng tin cậy 95% của mức tăng hoàn toàn dương so với bản liền trước. Khoảng của Precision và Recall còn chứa 0. Tập kiểm tra lịch sử đã được xem trong các vòng cải tiến; cần thêm giai đoạn dữ liệu tương lai để xác nhận mức cải thiện ổn định.

## Dữ liệu và tính toàn vẹn

**Không huấn luyện lại, không tính lại chất lượng mô hình trong lần nâng cấp giao diện này.** Giữ nguyên dữ liệu nguồn, thuật toán, bundle, hai SQLite phục vụ, pointer và các báo cáo đánh giá. Mô phỏng trong tab không sửa `events.csv` hoặc tập đánh giá.

Ở lần sửa dashboard trước, KPI trong gói gốc còn mang `run_id` của snapshot `session-20261022-1327`, khiến API từ chối ghép với run hiện tại. Liên kết metadata đã được sửa sau khi tính lại trực tiếp `events.csv` và xác nhận số liệu trùng khớp: 2.756.101 dòng thô, 460 dòng trùng, 0 dòng lỗi, 2.755.641 sự kiện sạch, 1.407.580 người dùng và 235.061 item. Đếm sự kiện: 2.664.218 xem, 68.966 thêm giỏ, 22.457 mua. Các giá trị đếm được giữ nguyên; kiểm tra chống ghép sai run của API vẫn còn.

`PACKAGE_SHA256.json` ở thư mục dự án đang làm việc giữ manifest nguồn để đối chiếu. **Bên trong ZIP cập nhật**, `PACKAGE_SHA256.json` ghi hash của nội dung đã cập nhật, `SOURCE_PACKAGE_SHA256.json` giữ manifest nguồn và `STOREFRONT_PACKAGE_INTEGRITY.json` ghi kết quả kiểm tra. Gói có đủ dữ liệu và snapshot phục vụ; không chứa dữ liệu Docker, cache hoặc các bản backup.

Bằng chứng cho nút mua và danh mục mới nằm tại `outputs/evidence/buy_now/`. Bằng chứng kiểm tra giao diện cửa hàng trước nằm tại `outputs/evidence/storefront/`. Bằng chứng dashboard trước vẫn ở `outputs/evidence/dashboard_studio/`.
