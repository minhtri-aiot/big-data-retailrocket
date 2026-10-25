# ALS và Graph trên dữ liệu RetailRocket

Quyết định dự án đã cố định: **Graph-based Session Recommendation là mô hình chính**. So sánh nhằm giải thích đặc tính và đo chất lượng; dù ALS có metric cao hơn, pipeline cũng không tự chuyển sang ALS.

## Đặc điểm dữ liệu và mục tiêu

RetailRocket chứa lượt xem, thêm giỏ và transaction, không phải bảng rating đầy đủ. Phần lớn ô user–item không được quan sát; một ô trống không đồng nghĩa người dùng không thích. Nhiều visitor ít lịch sử, session ngắn, item đuôi dài ít tương tác. Mục tiêu hiện tại là các item mới khách sẽ tương tác **trong phần còn lại của phiên**; không gán nhãn tất cả đều là ý định mua.

| Khía cạnh | Implicit ALS (Spark MLlib) | Graph Item–Item + Transition |
|---|---|---|
| Biểu diễn | Vector tiềm ẩn user/item, confidence từ hành vi | Cạnh đồng xuất hiện và cạnh chuyển tiếp giữa item |
| Điểm mạnh | Khai thác cấu trúc toàn cục, có thể tổng quát qua latent factors dù không có cặp đồng xuất hiện trực tiếp; MLlib phân tán | Tận dụng ngay seed trong phiên, dùng được với visitor ẩn danh; giải thích được qua cạnh; API tra chỉ mục nhỏ |
| Dữ liệu thưa | Được thiết kế xử lý ma trận thưa/implicit, regularization giúp chống quá khớp; không thể kết luận thưa đồng nghĩa ALS không dùng được | Gộp bằng chứng qua nhiều visitor làm item graph hữu ích, nhưng item hiếm vẫn thiếu/không có cạnh; sparse graph cũng có thể phân mảnh |
| User cold-start, chưa có gì | Không có user factor, cần fallback | Không có seed, cũng cần fallback |
| User mới, đã xem item quen | ALS lịch sử không có user factor; cần fold-in/retrain/hybrid để tận dụng context | Scorer có thể dùng cạnh của seed ngay, không cần user factor |
| Item cold-start, chưa có tương tác | Không có item factor | Không có cạnh vào/ra; không tự giải quyết được |
| Thứ tự và ý định ngắn hạn | ALS chuẩn trên tổng hợp user–item không mô hình hóa thứ tự; cần bổ sung thiết kế | Transition nắm bước chuyển kế tiếp, seed gần nhất có trọng số cao hơn |
| Độ phủ và thiên lệch | Dễ nghiêng về item phổ biến, phụ thuộc rank/alpha/regularization và dữ liệu | Dễ nghiêng về item phổ biến/cặp support cao; cap và top-neighbor bỏ mất quan hệ |
| Chi phí | Nhiều vòng giải nhân tố và xếp hạng trên catalog; hỗ trợ Spark sẵn | Self-join có thể lớn, cần cap20/top100; index disk và phiên bản phải đồng bộ |
| Giải thích | Latent factor khó diễn giải trực tiếp | Có thể trình bày seed/cạnh/trọng số; quan hệ không có nghĩa hai sản phẩm cùng loại |

ALS implicit coi các số đếm như độ tin cậy, không phải rating thật. Spark trả NaN với factor user/item chưa biết trong transform; coldStartStrategy=drop chỉ bỏ hàng NaN, **không giải quyết cold-start**. Trong đánh giá hiện tại, không bỏ query/label cold-start; dùng fallback như chính sách hệ thống và giữ các miss.

Lựa chọn Graph phù hợp mục tiêu phục vụ phiên ẩn danh và phản hồi theo hành vi vừa có, đồng thời thống nhất code đã có ở Phase 6. Đây là lý do kiến trúc, không phải bằng chứng Graph luôn có metric cao hơn ALS. Với lịch sử user dài và đủ dày, ALS vẫn có thể là baseline mạnh.

Số đo toàn nguồn của bản này: 1.407.580 visitor, 235.061 item, 2.145.179 cặp phân biệt; mật độ quan sát khoảng0,000648%, trung bình1,9577 sự kiện/visitor. Trong cohort test đã chọn,4.213/5.000 query thuộc visitor mới sau cutoff. Những số này lý giải giới hạn của ALS lịch sử trong giao thức phiên hiện tại; không phải một ngưỡng chung để loại ALS khỏi mọi bài toán.

## So sánh thực nghiệm của bản sửa

Xem [GRAPH_RESULTS.md](GRAPH_RESULTS.md). Lệnh `--compare-als` train Spark MLlib ALS từ chính lịch sử trước cutoff test. Cả hai chấm trên cùng query, target còn lại trong phiên, K, bộ lọc seen và fallback popularity. Không so số ALS static trước đây với số Graph session hiện tại.

Giới hạn công bằng cần nêu: baseline ALS dùng user factor đã học (rank16, reg0.1, alpha20, 5 vòng, seed42), chưa tune và không fold-in context. Graph dùng tối đa 5 seed phiên với cấu hình dự án 75/25. Bởi vậy phép đo là so **hai chính sách có thể triển khai** trên cùng bài toán, không chứng minh một họ thuật toán luôn ưu việt ở mọi thiết kế đầu vào. Nếu muốn một nghiên cứu mạnh hơn, đăng ký grid ALS trên validation, thêm fold-in hoặc factor-based anonymous session baseline, giữ test tương lai mới và bootstrap theo visitor. Không đưa cải tiến đó vào phục vụ chính.

Nếu Graph thấp, các hướng cải thiện Graph gồm tăng/chọn cap và top-neighbor theo validation; shrinkage/normalization cho item đuôi dài; recency theo phiên/khung thời gian; lọc bot; phân tích retrieval miss so với ranking miss; bổ sung metadata thật nếu có. Giữ cohort và báo cáo cả mức phủ lẫn chi phí, không loại lượt trượt hoặc sửa số.

## Nguồn phương pháp

- Apache Spark 3.5.6, [Collaborative Filtering](https://spark.apache.org/docs/3.5.6/ml-collaborative-filtering.html): latent factors, implicit confidence, regularization, cold-start NaN/drop.
- Hu, Koren, Volinsky (2008), [Collaborative Filtering for Implicit Feedback Datasets](https://doi.org/10.1109/ICDM.2008.22), nguồn thuật toán implicit ALS được Spark dẫn.
- [Session-rec research framework](https://github.com/saraLatifi/session-rec): các baseline item-kNN và mô hình tuần tự; nguồn tham khảo nghiên cứu, không phải code được sao chép vào bản sửa.
- Công thức Graph cụ thể và cap trong dự án được định nghĩa ở `pipeline/graph_training.py` và `session_transition.py`; không tuyên bố là công thức chuẩn duy nhất cho mọi hệ gợi ý graph.
