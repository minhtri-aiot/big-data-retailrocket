> **Cập nhật Graph thống nhất:** nội dung bên dưới là hướng dẫn/lịch sử trước khi thống nhất Phase 5–6. Dùng [Runbook hiện hành](GRAPH_RUNBOOK.md) và [kiểm thử bản mới](GRAPH_VALIDATION.md). Không dùng số liệu cũ làm kết quả bản mới.

# Chạy và trình bày demo gợi ý theo phiên

Bản nâng cấp thêm một nhánh gợi ý từ hành vi mới nhất trong phiên và một graph item-item có trọng số tương tác/độ mới. Nhánh đánh giá theo người dùng của bản trước được giữ riêng để so sánh. Điểm trong hai nhánh đo hai bài toán khác nhau; không ghép thành một bảng xếp hạng chung.

## 1. Mở lại demo trên máy đang có dữ liệu đã chạy

Mở Docker Desktop, chờ Linux engine sẵn sàng. Mở PowerShell:

```powershell
cd D:\BIGDATA\BigData_Phase_1_7
docker compose up -d --wait mongodb redis
docker compose up -d --no-deps --wait api
Invoke-RestMethod http://localhost:8000/health/ready
Start-Process http://localhost:8000/
```

Kiểm tra tên run và phiên bản mô hình trên trang. Để xem bằng API:

```powershell
Invoke-RestMethod http://localhost:8000/api/metrics | ConvertTo-Json -Depth 12
```

Các lệnh này chỉ khởi động dịch vụ cần để phục vụ bundle đã xuất. Không cần chạy lại full training mỗi lần mở demo. Nếu API báo chưa có model, kiểm tra `outputs/latest_run.json` và thư mục tương ứng trong `outputs/runs/`; máy mới cần thực hiện bước 2.

## 2. Từ ZIP cập nhật trên máy mới: chạy demo ngay

Giải nén toàn bộ thư mục dự án, ví dụ `D:\BIGDATA\BigData_Phase_1_7`. Docker Desktop phải chạy Linux containers; Docker cần truy cập mạng để lấy image/dependency trong lần đầu. ZIP cập nhật kèm **snapshot phục vụ demo** đã xuất, vì vậy có thể mở gợi ý mà không huấn luyện lại:

```powershell
cd D:\BIGDATA\BigData_Phase_1_7
.\scripts\run_session_demo.ps1 -OpenBrowser
```

Script build image, khởi động **MongoDB, Redis và API**, kiểm tra readiness/version, rồi thử hai request theo phiên và xác nhận lọc item vừa xem/thêm giỏ. Script không bật HDFS, Kafka, Spark hay Airflow và không tắt container khác. Lần sau, nếu image của đúng bản ZIP đã build và mã chưa đổi, có thể dùng:

```powershell
.\scripts\run_session_demo.ps1 -SkipBuild -OpenBrowser
```

Snapshot gồm `outputs/latest_run.json`, bundle đang chọn, `seen_items.sqlite`, `session_neighbors.sqlite`, metric/bằng chứng và báo cáo lineage tối thiểu. API mount `outputs` ở chế độ chỉ đọc; `.dockerignore` loại `outputs` và CSV raw khỏi Docker build context nên graph không bị nhét vào image. Dữ liệu runtime này đủ để phục vụ gợi ý và xem báo cáo, **không phải trạng thái đầy đủ của pipeline train**.

Thư mục ZIP cũng có mã các phase, CSV nguồn đầy đủ và bản sao bằng chứng trong `example_results/`. ZIP không kèm Docker volume HDFS, bảng Delta runtime, Spark ALS model hoặc checkpoint stream. MongoDB/Redis trên máy mới bắt đầu với trạng thái riêng; bộ lọc lịch sử đã đóng gói vẫn đọc SQLite. Các sự kiện demo gửi qua API stateless không làm thay đổi nhãn/chỉ số offline.

Nếu PowerShell chặn script do execution policy:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_session_demo.ps1 -OpenBrowser
```

Lệnh trên chỉ áp dụng cho tiến trình hiện tại. Nếu script báo thiếu serving snapshot, kiểm tra đã giải nén đầy đủ ZIP Session và giữ đúng cấu trúc `outputs/runs/<RunId>/serving/`; không lấy riêng một thư mục source từ bản ZIP cũ để chạy quick demo.

### Muốn chạy lại toàn bộ pipeline từ CSV

Snapshot demo không thay thế việc dựng lake/huấn luyện. Để kiểm chứng lại pipeline trên máy mới, cần dung lượng cho HDFS, model và scratch, rồi chạy lần lượt:

```powershell
cd D:\BIGDATA\BigData_Phase_1_7
.\scripts\run_full_training.ps1
.\scripts\run_neighborhood_upgrade.ps1
.\scripts\run_session_upgrade.ps1
```

Chạy lần lượt; chỉ sang lệnh tiếp theo sau khi lệnh trước hoàn tất. Không mở đồng thời nhiều job Spark trên máy RAM 16 GB. Nguồn được kiểm tra có **2.756.101 dòng raw**, còn **2.755.641 event sạch** sau khử trùng lặp. Đây là chế độ full history, không phải giới hạn demo 10.000 dòng. Mô hình vẫn có giới hạn lịch sử/ứng viên được ghi trong kế hoạch để kiểm soát bộ nhớ; “full data” không có nghĩa mọi event nhận trọng số giống nhau.

Nếu PowerShell chặn một script training, có thể chạy riêng script trong một tiến trình, ví dụ:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_session_upgrade.ps1
```

Lệnh trên không đổi execution policy toàn máy.

## 3. Chạy lại thí nghiệm từ full runtime đang có

Trên máy hiện tại, không cần nhập CSV và huấn luyện ALS lại. Script tự đi theo lineage để tìm nguồn full và item-item đối chứng:

```powershell
cd D:\BIGDATA\BigData_Phase_1_7
.\scripts\run_session_upgrade.ps1
```

Có thể chỉ rõ run để tái lập đúng đối chứng:

```powershell
.\scripts\run_session_upgrade.ps1 `
  -SourceRun full-20261022-1031 `
  -ReferenceRun neighbors-20261022-1233 `
  -RunId ('session-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
```

ID ví dụ thuộc máy đã chạy; máy mới dùng ID mà hai script trước vừa tạo, hoặc để tự tìm. Không tái sử dụng một `RunId` đã tồn tại. Script kiểm tra prerequisites, build image, bật HDFS/MongoDB/Redis, chạy pipeline, kiểm toán artifact, rồi khởi động lại API bằng image mới. Spark dùng 4 CPU, driver 3 GB và scratch trong filesystem Linux của Docker. Log được giữ ở `outputs/evidence/<RunId>-*.log`; báo cáo ở `outputs/runs/<RunId>/metrics/`.

Ví dụ xem log ở cửa sổ PowerShell khác khi pipeline đang chạy:

```powershell
Get-ChildItem .\outputs\evidence\session-*-training.log | Sort-Object LastWriteTime -Descending | Select-Object -First 1
Get-Content -LiteralPath .\outputs\evidence\TEN-RUN-training.log -Tail 30 -Wait
```

Thay `TEN-RUN` bằng ID thực tế. Không sửa `latest_run.json` bằng tay để ép kích hoạt thí nghiệm không vượt điều kiện đã đăng ký. Trạng thái `completed_not_promoted`, nếu xuất hiện, là thí nghiệm đã chạy nhưng không thay demo đang dùng; đọc báo cáo thay vì chọn lại cấu hình theo test.

## 4. Demo trên giao diện

1. Mở `http://localhost:8000/`. Xác nhận run/version đang phục vụ và các chỉ số đánh giá thông thường.
2. Thử nhánh gợi ý theo người dùng với một ID trong danh sách mẫu. ID ngoài cache có thể nhận popularity fallback; giao diện/API phải ghi nguồn gợi ý tương ứng.
3. Mở phần gợi ý theo phiên. Bắt đầu một phiên trống, rồi ghi nhận hành vi xem một sản phẩm.
4. Lấy gợi ý theo phiên. Ghi nhận tiếp hành vi thêm giỏ hoặc giao dịch với một sản phẩm khác rồi lấy gợi ý lại. So sánh danh sách, số hành vi trong context và nguồn gợi ý. Các item đã có trong context phải được loại khỏi kết quả.
5. Xóa context/bắt đầu phiên mới để quay lại trạng thái ban đầu. Một context không có hàng xóm phù hợp có thể nhận popularity fallback; đó là một kết quả hợp lệ cần trình bày đúng nhãn.

Các thao tác này là **mô phỏng hành vi phục vụ demo**, không phải giao dịch thương mại thật. Chúng không được thêm vào Silver hay dùng thay nhãn test, không tự làm thay đổi chỉ số offline. Điểm xếp hạng không phải xác suất khách hàng mua hàng.

## 5. Gọi API theo phiên bằng PowerShell

API stateless: gửi toàn bộ context được phép dùng tại thời điểm gợi ý trong mỗi request. Không dựa vào một `visitorid` mới để giả định server đã tự biết lịch sử phiên. `visitorid` là tùy chọn; nếu có, API còn lọc lịch sử đã biết của người dùng đó.

Ví dụ lấy một seed thực có trong danh sách phổ biến rồi ghi nhận hành vi xem:

```powershell
$initial = Invoke-RestMethod 'http://localhost:8000/api/recommendations/2147483647?k=10'
$seedItem = [int]$initial.items[0].itemid
$context = @(@{itemid = $seedItem; event = 'view'})
$body = @{events = $context; k = 10} | ConvertTo-Json -Depth 6
$first = Invoke-RestMethod -Method Post `
  -Uri 'http://localhost:8000/api/session/recommendations' `
  -ContentType 'application/json' -Body $body
$first | ConvertTo-Json -Depth 10
```

Tiếp tục cùng context bằng một hành vi thêm giỏ:

```powershell
$nextItem = [int]$first.items[0].itemid
$context += @{itemid = $nextItem; event = 'addtocart'}
$body = @{events = $context; k = 10} | ConvertTo-Json -Depth 6
$second = Invoke-RestMethod -Method Post `
  -Uri 'http://localhost:8000/api/session/recommendations' `
  -ContentType 'application/json' -Body $body
$second | ConvertTo-Json -Depth 10
```

Ba loại `event` là `view`, `addtocart`, `transaction`. Có thể gửi `timestamp` dạng epoch millisecond cho từng event nếu replay một context có thứ tự thời gian. Timestamp và giới hạn kích thước context được API kiểm tra; không đưa hành vi xảy ra sau thời điểm cần dự đoán vào context. Nếu bỏ timestamp, server xử lý theo quy ước được công bố trong response/cấu hình. Để xóa phiên stateless, gửi `events = @()` hoặc bắt đầu lại biến `$context`.

Đọc `run_id`, `model_version`, `source`, thông tin context và danh sách `items` trong response. Với context không có hàng xóm, fallback vẫn có thể tạo đủ top-K. Không lấy một ví dụ đẹp trên giao diện làm bằng chứng thay thế tập test.

## 6. Đọc kết quả mà không nhầm hai cách đánh giá

| Nội dung | Đánh giá thông thường đã có | Đánh giá theo phiên mới |
|---|---|---|
| Câu hỏi | Trong giai đoạn tương lai, user sẽ tương tác item mới nào? | Sau context đã xảy ra trong phiên, item mới tiếp theo là gì? |
| Đơn vị tính trung bình | User | Query phiên theo kế hoạch |
| Nhãn đúng | Tập item tương lai mới với user | Một next-new-item cho mỗi query |
| Context được dùng | Lịch sử trước cutoff train | Chỉ hành vi có trước target của query |
| K | 10 | 10 |
| Đối chứng | Bản item-item trước và popularity 7 ngày, trên cùng user | Item-item Jaccard trước dùng cùng past context và popularity 7 ngày, trên cùng query |

Trong bài toán có **một target**, hit ở top-10 đóng góp `1/10` cho Precision@10 và `1` cho Recall@10. Vì vậy Precision@10 tối đa là **10%**, Recall@10 chính là hit rate; NDCG@10 còn xét vị trí target. Không so trực tiếp điểm Recall theo phiên với Recall theo user có nhiều target rồi kết luận mô hình tăng tương ứng. So sánh hợp lệ là hai mô hình trên **cùng protocol, cùng tập query, cùng K và cùng dữ liệu quá khứ**.

Phiên lịch sử được tách theo khoảng nghỉ 30 phút theo kế hoạch; khung hình thời gian bằng nhau được xử lý để không lộ hành vi cùng/ở sau thời điểm target. Graph của mỗi phép chấm chỉ được fit bằng phần train trước cutoff; context xuất hiện sau cutoff chỉ là thông tin đã quan sát của query, không cập nhật graph bằng nhãn tương lai. Serving có thể được refit trên toàn bộ lịch sử sau khi chấm xong; không dùng bản refit này để chấm lại test.

Các nguyên tắc cần kiểm tra trong báo cáo:

- Cấu hình được chọn bằng validation thời gian và được đóng băng trước test. Không thử tiếp nhiều cấu hình để tìm một điểm test đẹp.
- Không lấy mẫu negative item để thu hẹp catalog. Target cold/chưa tới được bằng graph vẫn ở mẫu số và được tính miss.
- Điều kiện chọn query “next-new-item” được công bố; số query/số user/số target cold và phần bị loại theo định nghĩa phải được báo cáo. Đây là một nhiệm vụ riêng, không thay thế thống kê user cold trong đánh giá cũ.
- Popularity, item-item cũ và item-item mới dùng cùng context đã biết, cùng bộ lọc seen và cùng query. Không so API cache cũ thiếu context với mô hình mới có context rồi gán toàn bộ mức tăng cho thuật toán.
- Khoảng tin cậy và mức chênh lệch được báo cáo nếu có. Nhiều query từ cùng user không phải các quan sát độc lập hoàn toàn; cách resample phải được ghi rõ.
- Tập test RetailRocket này đã xuất hiện ở các lượt phát triển trước. Không gọi kết quả là benchmark tương lai chưa từng được xem hoặc bằng chứng tăng CTR/doanh thu.

## 7. Bằng chứng nên mang khi trình bày

Trong `outputs/runs/<RunId>/metrics/`, đọc kế hoạch thực nghiệm, lựa chọn validation, so sánh test riêng cho mỗi protocol, ranking từng query/user và báo cáo kiểm toán độc lập. Trong `outputs/evidence/`, giữ log build/train/audit và JSON smoke API do runner tạo. Bản ZIP chia sẻ có bản sao báo cáo ở `example_results/`; thư mục này là bằng chứng một lần chạy, không phải runtime đang phục vụ.

`serving/session_neighbors.sqlite` là chỉ mục hàng xóm để API truy vấn các seed của phiên, còn `serving/model_bundle.json` mô tả model/version và tham số. `seen_items.sqlite` lọc lịch sử đã quan sát. Không suy từ một SQLite graph toàn catalog rằng mọi item đều có hàng xóm hoặc mọi user đều có tín hiệu cá nhân hóa; coverage/fallback phải đọc từ artifact và response.

Kiểm tra SHA-256 của ZIP với tệp `.sha256` đi kèm trước khi chuyển sang máy khác:

```powershell
Get-FileHash 'D:\BIGDATA\BigData_Phase_1_7_Session_Updated.zip' -Algorithm SHA256
```

Khi trình bày, nên nói rõ: “Nhánh theo phiên dùng hành vi vừa phát sinh để xếp hạng. Tôi giữ đánh giá theo user của bản cũ, báo cáo riêng next-new-item theo phiên, và so các thuật toán trên cùng thông tin quá khứ. Kết quả offline chưa phải tỷ lệ mua hàng thật.”
