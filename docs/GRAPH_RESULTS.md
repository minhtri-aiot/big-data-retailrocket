# Kết quả thực đo của bản Graph thống nhất

Run **graph-full-20261024**, model **graph-graph-full-20261024-v1**. Nguồn là events.csv trong Release storefront-v2, ZIP gốc SHA-256 `0bcab83532482f9934078c525c2be51eb44ded50aae1a8f2f8fc2e0a41f74fa9` đã đối chiếu trước khi giải nén. CSV SHA-256 `3745aa83238b1e6d44d8fda209807899f420084398f94ddf745f3cbcfecbf9e7`. Đây là kết quả trainer Spark mới, không lấy lại bảng số liệu cũ.

## Toàn bộ nguồn RetailRocket

2,756,101 dòng gốc → **2,755,641 sự kiện sạch**, 1,407,580 visitor, 235,061 item; 2,145,179 cặp user–item phân biệt. Trung bình 1.9577 sự kiện/visitor; mật độ quan sát ma trận user–item khoảng **0.000648%**. Đây là thống kê độ thưa, không tự chứng minh thuật toán nào tốt hơn.

Mốc epoch milliseconds 50/70/85%: `[1436450667169, 1438557297020, 1440540443918]`. Graph test fit ≤1440540443918, query/targets ở phần sau. Hai validation fold và lựa chọn cố định75/25 được ghi trong graph_validation.json. Không dùng test để chuyển sang ALS.

Cùng **5.000 query của 4,759 visitor**, cùng labels là mọi item mới còn lại trong phiên, cùng K=10, cùng session-seen và popularity fallback:

| Mô hình | Precision@10 | Recall@10 | NDCG@10 | HitRate@10 |
|---|---:|---:|---:|---:|
| Graph chính | 4.1260% | 28.2271% | 19.6734% | 34.4600% |
| ALS đối chứng | 0.1520% | 0.8446% | 0.4886% | 1.3600% |
| Phổ biến | 0.0740% | 0.4297% | 0.1990% | 0.7400% |

Graph cao hơn baseline ALS đã cấu hình **trong lần đo này**. Không kết luận Graph luôn tốt hơn ALS: baseline chỉ dùng rank16/reg0.1/alpha20/maxIter5, seed42, không tune hoặc fold-in; ALS dựa user factor lịch sử, Graph dùng context phiên. Có **4,213/5.000 query** từ visitor chưa xuất hiện trước cutoff; đây là điều kiện có lợi cho chính sách theo context so với ALS lịch sử. Không bỏ những query này khỏi baseline. Có 632 nhãn item chưa có trong catalog train trên tổng 10,279 nhãn; vẫn chấm đầy đủ. Cần validation tuning baseline và tập tương lai mới để kết luận nghiên cứu mạnh hơn.

Chỉ **38,875/274,409 phiên** trong kỳ đủ điều kiện có context và item mới về sau; lấy 5.000 theo hash trước chấm. 1,777 query có nhiều nhãn. Không suy rộng chỉ số này thành tỷ lệ mua hàng của mọi visitor. Tập lịch sử này đã xuất hiện trong quá trình phát triển trước; không coi là test tương lai chưa từng xem. Chưa thực hiện A/B test, bootstrap mức tăng hoặc kiểm định ý nghĩa cho phép so ALS mới này.

Sau khi cố định metrics, graph phục vụ refit **toàn bộ sự kiện sạch**: 2.671.950 cạnh Item–Item, 454.978 cạnh Transition. SQLite gồm cả context user nên khoảng180.5MiB; bản demo nén gzip để lưu trong repo. Cap20 và top100 vẫn áp dụng, không giữ tất cả cặp có thể có.

## Smoke trên mẫu thật 10.000 dòng

Mẫu này có 141 query test; không thay thế phép đo toàn nguồn ở trên:

| Mô hình | Precision@10 | Recall@10 | NDCG@10 | HitRate@10 |
|---|---:|---:|---:|---:|
| Graph chính | 0.5674% | 3.0260% | 2.5298% | 4.2553% |
| ALS đối chứng | 0.4965% | 2.4823% | 1.7179% | 3.5461% |
| Phổ biến | 0.0000% | 0.0000% | 0.0000% | 0.0000% |

## Kiểm tra độc lập

`python -m scripts.verify_graph` đã tính lại P/R/NDCG/HitRate từ toàn bộ 5.000 dòng `test_rankings.jsonl`, xác nhận context trước target, labels mới và không gợi ý seen/trùng. Các giá trị khớp bundle trong sai số1e-12. Kiểm checksum và model/scorer version cùng đạt. Xem `reports/verification/graph_unified/summary.json` và `demo/graph_snapshot/metrics/`.

Snapshot demo đi kèm chính là model full trên; không chứa raw CSV hoặc clean/ storage của run tích hợp. Dùng để chạy API ngay; muốn train/retrain toàn nguồn cần CSV gốc hoặc Delta Silver và hạ tầng tương ứng.
