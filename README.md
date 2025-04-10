# PoC Hệ thống Điều phối Tài xế (Ride-Hailing Dispatch)

## Giới thiệu

Đây là một dự án Proof of Concept (PoC) mô phỏng và đánh giá hiệu năng của thành phần cốt lõi trong hệ thống đặt xe (ride-hailing): **hệ thống điều phối (dispatch system)**, cụ thể là chức năng tìm kiếm và khớp (matching) một tài xế phù hợp cho một yêu cầu chuyến đi của khách hàng.

Mục tiêu chính của PoC này là:

1.  **Đo lường thời gian phản hồi:** Xác định thời gian cần thiết để tìm và gán một tài xế cho một yêu cầu chuyến đi, đặc biệt dưới các tải khác nhau. Mục tiêu cụ thể là kiểm tra khả năng đáp ứng dưới 5 giây cho mỗi yêu cầu.
2.  **Đánh giá thuật toán khớp lệnh cơ bản:** Sử dụng thuật toán tìm kiếm tài xế gần nhất và sẵn sàng.
3.  **Mô phỏng xử lý đồng thời:** Kiểm tra cách hệ thống xử lý nhiều yêu cầu chuyến đi đến cùng một lúc.
4.  **Xuất kết quả trực quan:** Cung cấp kết quả dưới dạng file CSV và báo cáo HTML để dễ dàng phân tích.

## Cách hoạt động

Quy trình chính của PoC như sau:

1.  **Tải dữ liệu:** Đọc thông tin danh sách tài xế (vị trí, ID) từ `data/drivers.csv` và danh sách yêu cầu chuyến đi (vị trí đón/trả, ID khách hàng) từ `data/trips.csv` bằng thư viện **Pandas**.
2.  **Chuẩn bị tìm kiếm:** Xây dựng cấu trúc dữ liệu **KDTree** (từ thư viện **SciPy**) dựa trên vị trí của tất cả các tài xế hợp lệ. KDTree cho phép tìm kiếm không gian (spatial search) các tài xế gần một vị trí cho trước một cách hiệu quả.
3.  **Xử lý yêu cầu đồng thời:** Sử dụng **`concurrent.futures.ThreadPoolExecutor`** để tạo một nhóm luồng (thread pool), cho phép xử lý nhiều yêu cầu chuyến đi song song. Mỗi yêu cầu chuyến đi sẽ được xử lý bởi một luồng riêng.
4.  **Tìm kiếm tài xế cho mỗi chuyến:**
    - Đối với mỗi yêu cầu chuyến đi, sử dụng **KDTree** để truy vấn `k` tài xế gần nhất với vị trí đón khách (tham số `k_nearest` có thể điều chỉnh).
    - Tính toán khoảng cách **Haversine** (khoảng cách địa lý chính xác trên mặt cầu) giữa khách hàng và các tài xế ứng viên tìm được.
    - Lọc bỏ những tài xế nằm ngoài bán kính tìm kiếm tối đa (`max_driver_dist_km`).
    - Sắp xếp các tài xế ứng viên còn lại theo khoảng cách Haversine tăng dần.
5.  **Gán tài xế và Đảm bảo tính nhất quán:**
    - Sử dụng **`threading.Lock`** để bảo vệ một tập hợp (`set`) chứa ID các tài xế đã được gán (`assigned_drivers_set`).
    - Mỗi luồng xử lý chuyến đi, khi có danh sách tài xế ứng viên, sẽ cố gắng lấy lock.
    - Bên trong lock, luồng sẽ duyệt qua danh sách tài xế ứng viên (theo thứ tự gần nhất). Nếu tìm thấy một tài xế chưa có trong `assigned_drivers_set`, luồng sẽ **ngay lập tức** thêm ID tài xế đó vào set và chọn tài xế đó cho chuyến đi, sau đó nhả lock.
    - Cơ chế lock này đảm bảo rằng một tài xế chỉ được gán cho **duy nhất một chuyến đi**, tránh tình trạng race condition khi nhiều luồng cùng muốn chọn một tài xế.
6.  **Đo lường và Ghi kết quả:**
    - Ghi lại thời gian xử lý (response time) cho từng yêu cầu chuyến đi (tính bằng mili giây).
    - Xác định trạng thái khớp lệnh (`success`, `fail (all drivers busy)`, `fail (no driver nearby)`, `fail (timeout)`, v.v.).
    - Lưu trữ tất cả kết quả chi tiết (ID khách hàng, ID tài xế, khoảng cách, thời gian, trạng thái) vào DataFrame của **Pandas**.
7.  **Xuất báo cáo:**
    - Lưu DataFrame kết quả chi tiết thành file `data/dispatch_results.csv`.
    - Sử dụng thư viện **Jinja2** và một file template HTML (`templates/report_template.html`) để tạo ra một báo cáo trực quan (`data/dispatch_report.html`) bao gồm:
      - Bảng tóm tắt thống kê (tổng số chuyến, số tài xế, tỷ lệ thành công/thất bại, chi tiết lý do thất bại, thống kê thời gian phản hồi: trung bình, trung vị, P95, max).
      - Bảng chi tiết kết quả của từng chuyến đi.

## Các kỹ thuật và thư viện chính

- **Pandas:** Đọc/ghi CSV, thao tác DataFrame hiệu quả.
- **NumPy:** Tính toán số học nền tảng.
- **SciPy ( `scipy.spatial.KDTree` ):** Tối ưu hóa việc tìm kiếm các điểm (tài xế) lân cận trong không gian 2 chiều (latitude, longitude).
- **`math.haversine` (tự triển khai):** Tính khoảng cách địa lý chính xác giữa hai điểm tọa độ.
- **`concurrent.futures.ThreadPoolExecutor`:** Thực thi các tác vụ khớp lệnh song song trên nhiều luồng, mô phỏng xử lý đồng thời.
- **`threading.Lock`:** Đảm bảo an toàn luồng (thread-safety) khi cập nhật trạng thái tài xế đã được gán, ngăn chặn race conditions.
- **Jinja2:** Tạo báo cáo HTML động từ dữ liệu kết quả.
