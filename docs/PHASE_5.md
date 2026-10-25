# Phase 5 — xây dựng và đánh giá Graph bằng Apache Spark

Entry point chính: `pipeline.graph_training.train_model`. `pipeline.train.train_model` chuyển tiếp sang entry point này để tương thích import cũ. Hàm `train_als_legacy` chỉ giữ để tái tạo lịch sử; runner, full_train và worker không gọi nó.

## Đầu vào và tiền xử lý

Pipeline tích hợp đọc **Delta Silver được pin bằng versionAsOf**. Standalone CLI đọc CSV, Parquet hoặc Delta và lưu nguồn/checksum hoặc phiên bản Delta trong `graph_plan.json`. ID nguyên không âm 32 bit, timestamp epoch milliseconds dương, loại sự kiện hợp lệ; bản ghi transaction phải có transactionid hợp lệ nếu cột này hiện diện. Dữ liệu sai làm job thất bại, không bị bỏ im lặng. Reviews không tham gia mô hình. Loại trùng theo các trường gốc hiện có trước khi bỏ cột phụ.

Spark giữ các cột timestamp/visitorid/itemid/event để train. Không collect toàn bộ dữ liệu về NumPy/Pandas. Các phép groupBy, window, self-join, đếm support, chọn top hàng xóm đều do Spark SQL thực hiện. Đây là đồ thị quan hệ hành vi, **không phải GNN** và không yêu cầu GraphX/GraphFrames.

## Item–Item

Mỗi visitor đóng góp một lần cho một item, giữ tối đa 20 item phân biệt mới nhất (timestamp giảm dần; hòa thì item ID tăng). Gọi n(A) là số visitor có A sau cap, n(A,B) là số visitor có cả A và B:

`co(A,B) = n(A,B)/n(A) × n(A,B)/(n(A,B)+3)`.

Cạnh có hướng vì mẫu số là support của A. Giữ top 100 cạnh/seed, score giảm rồi ID tăng. Cap giúp chặn chi phí self-join, đồng thời có thể làm mất quan hệ dài hạn; không tuyên bố giữ mọi cặp lịch sử.

## Session Transition

Spark nhóm timestamp theo visitor, tạo phiên mới nếu gap **>30 phút**. Các item đồng timestamp là một nhóm. Từ mỗi nhóm, nối từng item A sang từng item B **mới trong phiên** ở nhóm timestamp kế tiếp. Đếm số lần A→B; top 100 cạnh/seed. Không nối đồng timestamp, không nối xuyên gap và không nối đến item đã xuất hiện trước trong cùng phiên. `score` của nhánh này là số lần chuyển tiếp, chuẩn hóa khi xếp hạng.

## Xếp hạng

Chọn tối đa 5 seed phân biệt gần nhất trong 100 sự kiện context. Trọng số sự kiện 1/1/1, không decay theo đồng hồ, affinity theo vị trí `0.8^r`. Mỗi seed/nhánh chia điểm cạnh cho cạnh lớn nhất rồi lấy căn bậc hai:

`score(B) = Σ affinity(A) × [0.75 sqrt(co(A,B)/max_co(A)) + 0.25 sqrt(tr(A,B)/max_tr(A))]`.

Nhánh thiếu cạnh đóng góp 0. Loại seen; hòa score thì ID tăng; bù phổ biến nếu chưa đủ K. Điểm là độ liên quan, không phải xác suất. Chính `score_context` được dùng để chấm offline và trả API.

## Chia dữ liệu và giao thức

Mốc xấp xỉ phân vị timestamp của các dòng sự kiện 50/70/85%, sai số phân vị ≤0.001; giữ timestamp bằng nhau cùng phía. Hai validation folds 50→70 và 70→85; fit test đến 85%, chấm đoạn sau. Mặc định cấu hình cố định 75/25; full-grid thử transition weight 0.1/0.25/0.4/0.5, chọn trung bình validation NDCG rồi Recall rồi Precision. Không dùng test chọn họ mô hình hoặc chuyển sang ALS.

Mỗi phiên tối đa một query tại nhóm timestamp mới đầu tiên sau cutoff có context quá khứ. Label gồm mọi item **mới trong phiên** từ thời điểm này đến cuối phiên/kỳ đánh giá. Context chỉ có timestamp nhỏ hơn target; seen là toàn bộ item trước target của phiên, dù context truyền scorer chặn 100 sự kiện. Lấy tối đa 5.000 query theo SHA256(visitorid:session_start:42), trước khi chấm. Phiên đơn sự kiện không đủ điều kiện; report ghi số active/eligible để tránh diễn giải thành kết quả cho toàn bộ khách.

Cold user/item vẫn nằm trong query/labels. Không negative sampling, không loại miss do không có cạnh. Graph chỉ fit lịch sử trước cutoff, popularity cũng chỉ lấy lịch sử đó. ALS benchmark (khi yêu cầu) dùng chính cutoff/cohort/labels/K/seen/fallback; khác biệt đầu vào được công bố: ALS dùng user factor lịch sử, Graph dùng context phiên. ALS baseline không tune và không fold-in user mới từ context. Không xem đây là bằng chứng thuần túy tách biệt hai thuật toán ở điều kiện thông tin giống hệt nhau.

Các metric macro/query: Precision = hit/K; Recall = hit/số label; NDCG relevance nhị phân với chiết khấu log2; HitRate = 1 nếu query có ít nhất một hit, ngược lại 0. Không query thì metric null, status not_evaluable. HitRate không đồng nghĩa Recall ở bài toán nhiều label.

## Artifact

| Tệp trong run | Nội dung |
|---|---|
| `models/validation-*.sqlite` | Các đồ thị chỉ fit train của validation fold |
| `models/evaluation_graph.sqlite` | Đồ thị fit trước cutoff test |
| `metrics/graph_plan.json`, `graph_validation.json` | Nguồn/split/cấu hình/chọn validation |
| `metrics/test_rankings.jsonl` | Context, seen, label, prediction từng query; tự tính lại metric |
| `metrics/model_metrics.json`, `kpis.json` | Kết quả thực đo và số đếm nguồn |
| `serving/model_bundle.json` | algorithm=graph_session, version, config, popularity, metrics |
| `serving/session_neighbors.sqlite` | Bảng neighbors, transitions, user_context, metadata phiên bản |
| `serving/seen_items.sqlite` | Toàn bộ cặp lịch sử để lọc seen khi có visitor ID |
| `serving/manifest.json` | SHA-256 bundle và SQLite |

Metrics được cố định trước refit toàn nguồn. `model_version` của bundle và scorer bằng nhau; `metric_model_version` ghi graph đánh giá trước cutoff. Artifact mới nằm trong run mới; chỉ đổi pointer sau khi job và các bước bắt buộc thành công. Runner còn yêu cầu registry MongoDB/Redis thành công. API xác thực checksum/index metadata trước khi dùng run mới.
