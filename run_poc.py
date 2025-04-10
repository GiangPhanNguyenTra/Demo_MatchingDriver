# run_poc.py
import pandas as pd
import time
import os
from datetime import datetime # Thêm để lấy thời gian tạo báo cáo
from src.dispatcher import dispatch_drivers_locked # Import hàm điều phối chính
from jinja2 import Environment, FileSystemLoader # Thêm Jinja2

# --- Các hằng số cấu hình (giữ nguyên) ---
DATA_DIR = 'data'
DRIVERS_CSV_FILENAME = 'drivers.csv'
TRIPS_CSV_FILENAME = 'trips.csv'
RESULTS_CSV_FILENAME = 'dispatch_results.csv'
HTML_REPORT_FILENAME = 'dispatch_report.html' # Tên file HTML output
TEMPLATE_DIR = 'templates' # Thư mục chứa template
TEMPLATE_NAME = 'report_template.html' # Tên file template

DRIVERS_CSV_PATH = os.path.join(DATA_DIR, DRIVERS_CSV_FILENAME)
TRIPS_CSV_PATH = os.path.join(DATA_DIR, TRIPS_CSV_FILENAME)
RESULTS_CSV_PATH = os.path.join(DATA_DIR, RESULTS_CSV_FILENAME)
HTML_REPORT_PATH = os.path.join(DATA_DIR, HTML_REPORT_FILENAME) # Đường dẫn file HTML output

MAX_WORKERS_THREADS = None

# --- Hàm Lưu Kết Quả CSV (giữ nguyên) ---
def save_results_to_csv(results_df, output_path):
    # ... (code hàm này giữ nguyên như trước) ...
    if results_df is None or results_df.empty:
        print("Result DataFrame is empty or None. Nothing to save.")
        return False # Trả về False nếu không lưu được

    # Đảm bảo thư mục tồn tại
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
            print(f"Created directory: {output_dir}")
        except OSError as e:
            print(f"Error creating directory {output_dir}: {e}")
            return False # Không thể tạo thư mục, không lưu

    try:
        # Đổi tên cột để khớp với yêu cầu 6 cột ban đầu
        results_df_renamed = results_df.rename(columns={
            'distance_km': 'distance',
            'driver_customer_distance_km': 'driver_customer_distance',
            'response_time_ms': 'response_time' # Đơn vị vẫn là ms
        })

        # Chỉ giữ lại các cột cần thiết theo yêu cầu
        output_columns = ['id_customer', 'id_driver', 'distance', 'driver_customer_distance', 'response_time', 'status']
        # Lấy các cột tồn tại trong df sau khi đổi tên
        existing_output_columns = [col for col in output_columns if col in results_df_renamed.columns]

        if len(existing_output_columns) != len(output_columns):
                print(f"Warning: Not all expected output columns found after renaming. Saving available columns: {existing_output_columns}")


        results_df_final = results_df_renamed[existing_output_columns]

        results_df_final.to_csv(output_path, index=False, float_format='%.3f') # Định dạng số thập phân
        print(f"\nDispatch results saved to: {output_path}")
        return True # Trả về True nếu lưu thành công

    except KeyError as e:
        print(f"\nError saving CSV: Column {e} not found in results DataFrame after renaming.")
        if 'results_df' in locals(): print("Columns available before rename:", results_df.columns)
        if 'results_df_renamed' in locals(): print("Columns available after rename attempt:", results_df_renamed.columns)
        return False
    except Exception as e:
        print(f"\nError saving results to CSV {output_path}: {e}")
        return False


# --- Hàm MỚI: Tạo báo cáo HTML ---
def generate_html_report(results_df, template_dir, template_name, output_html_path, num_drivers):
    """
    Tạo file báo cáo HTML từ DataFrame kết quả sử dụng Jinja2 template.
    """
    if results_df is None or results_df.empty:
        print("No results data to generate HTML report.")
        return

    print(f"\nGenerating HTML report...")

    # 1. Chuẩn bị dữ liệu cho template
    # Dữ liệu chi tiết (chuyển thành list of dicts)
    # Đảm bảo các cột cần thiết cho HTML tồn tại trước khi chuyển đổi
    report_columns = ['id_customer', 'id_driver', 'distance', 'driver_customer_distance', 'response_time', 'status']
    # Đổi tên cột trong bản sao để không ảnh hưởng df gốc nếu cần tính toán thêm
    results_for_html = results_df.copy()
    results_for_html = results_for_html.rename(columns={
            'distance_km': 'distance',
            'driver_customer_distance_km': 'driver_customer_distance',
            'response_time_ms': 'response_time'
        })
    # Lấy các cột thực sự tồn tại
    available_report_columns = [col for col in report_columns if col in results_for_html.columns]
    results_list = results_for_html[available_report_columns].where(pd.notna(results_for_html), None).to_dict('records')

    # 2. Tính toán Tóm tắt (summary)
    summary = {}
    summary['total_drivers_available'] = num_drivers 
    summary['total_trips'] = len(results_df)
    summary['success_count'] = results_df[results_df['status'] == 'success'].shape[0]
    summary['total_fails'] = summary['total_trips'] - summary['success_count']
    summary['success_rate'] = summary['success_count'] / summary['total_trips'] if summary['total_trips'] > 0 else 0
    # Chi tiết lỗi
    fail_counts = results_df[results_df['status'] != 'success']['status'].value_counts().to_dict()
    summary['fails_by_reason'] = fail_counts

    # Thống kê thời gian (lấy từ cột gốc response_time_ms)
    valid_times = results_df['response_time_ms'].dropna()
    valid_times = valid_times[valid_times >= 0]
    if not valid_times.empty:
        summary['avg_response_time'] = valid_times.mean()
        summary['median_response_time'] = valid_times.median()
        summary['p95_response_time'] = valid_times.quantile(0.95)
        summary['max_response_time'] = valid_times.max()
    else:
         summary['avg_response_time'] = None # Đặt là None nếu không có dữ liệu

    success_times = results_df[results_df['status'] == 'success']['response_time_ms'].dropna()
    success_times = success_times[success_times >= 0]
    if not success_times.empty:
         summary['avg_success_response_time'] = success_times.mean()
         summary['median_success_response_time'] = success_times.median()
         summary['p95_success_response_time'] = success_times.quantile(0.95)
         summary['max_success_response_time'] = success_times.max()
    else:
         summary['avg_success_response_time'] = None


    # 3. Render template
    try:
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template(template_name)

        # Thời gian tạo báo cáo
        generation_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        context = {
            'results': results_list,
            'summary': summary,
            'generation_time': generation_time_str
        }

        html_content = template.render(context)

        # 4. Lưu file HTML
        with open(output_html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"HTML report successfully generated: {output_html_path}")

    except ImportError:
         print("Error: Jinja2 library not found. Please install it: pip install Jinja2")
    except Exception as e:
        print(f"Error generating HTML report: {e}")


# --- Main Execution (Cập nhật để gọi hàm generate_html_report) ---
if __name__ == '__main__':
    print("--- Starting Ride Hailing Dispatch PoC from CSV Data ---")
    start_poc_time = time.time()

    # 1. Tải dữ liệu từ CSV
    print(f"\n[Phase 1: Loading Data]")
    drivers_data = None
    trips_data = None
    # ... (Phần code load data giữ nguyên) ...
    try:
        drivers_data = pd.read_csv(DRIVERS_CSV_PATH)
        # Chuyển đổi kiểu dữ liệu để đảm bảo tính nhất quán
        drivers_data['lat_driver'] = pd.to_numeric(drivers_data['lat_driver'], errors='coerce')
        drivers_data['lon_driver'] = pd.to_numeric(drivers_data['lon_driver'], errors='coerce')
        drivers_data['id_driver'] = drivers_data['id_driver'].astype(str) # ID thường là string hoặc int lớn
        print(f"Successfully loaded {len(drivers_data)} drivers from {DRIVERS_CSV_PATH}")
    except FileNotFoundError:
        print(f"Error: Drivers file not found at {DRIVERS_CSV_PATH}")
    except Exception as e:
        print(f"Error loading or processing drivers CSV: {e}")

    try:
        trips_data = pd.read_csv(TRIPS_CSV_PATH)
        # Chuyển đổi kiểu dữ liệu
        trips_data['lat_start'] = pd.to_numeric(trips_data['lat_start'], errors='coerce')
        trips_data['lon_start'] = pd.to_numeric(trips_data['lon_start'], errors='coerce')
        trips_data['lat_end'] = pd.to_numeric(trips_data['lat_end'], errors='coerce')
        trips_data['lon_end'] = pd.to_numeric(trips_data['lon_end'], errors='coerce')
        trips_data['id_customer'] = trips_data['id_customer'].astype(str)
        print(f"Successfully loaded {len(trips_data)} trips from {TRIPS_CSV_PATH}")
    except FileNotFoundError:
        print(f"Error: Trips file not found at {TRIPS_CSV_PATH}")
    except Exception as e:
        print(f"Error loading or processing trips CSV: {e}")


    # 2. Thực hiện điều phối
    print("\n[Phase 2: Dispatching Drivers]")
    results_data = None # Khởi tạo
    if drivers_data is not None and not drivers_data.empty and trips_data is not None and not trips_data.empty:
        results_data, num_valid_drivers_used  = dispatch_drivers_locked(
            trips_df=trips_data,
            drivers_df=drivers_data,
            max_workers=MAX_WORKERS_THREADS
        )
    else:
        print("Skipping dispatch phase due to missing or empty input data.")
        if trips_data is not None and not trips_data.empty and (drivers_data is None or drivers_data.empty):
            print("Attempting to process trips with no drivers...")
            results_data = dispatch_drivers_locked(trips_data, pd.DataFrame() if drivers_data is None else drivers_data)

    # 3. Lưu kết quả CSV
    print("\n[Phase 3: Saving Results]")
    csv_saved_successfully = False
    if results_data is not None and not results_data.empty:
        csv_saved_successfully = save_results_to_csv(results_data, output_path=RESULTS_CSV_PATH)
    else:
        print("No dispatch results generated or available to save.")

    # 4. Tạo báo cáo HTML (CHỈ KHI CÓ DỮ LIỆU VÀ CSV ĐÃ LƯU - tùy chọn)
    # Bạn có thể tạo HTML ngay cả khi CSV chưa lưu nếu muốn, nhưng thường thì nên có CSV trước
    print("\n[Phase 4: Generating HTML Report]")
    if results_data is not None and not results_data.empty:
         generate_html_report(
             results_df=results_data,
             template_dir=TEMPLATE_DIR,
             template_name=TEMPLATE_NAME,
             output_html_path=HTML_REPORT_PATH,
             num_drivers=num_valid_drivers_used 
         )
    else:
         print("Skipping HTML report generation as there are no results.")


    end_poc_time = time.time()
    print(f"\n--- PoC Finished ---")
    print(f"Total PoC execution time: {end_poc_time - start_poc_time:.2f} seconds")
    # In đường dẫn file report để dễ mở
    print(f"HTML report (if generated) can be found at: {os.path.abspath(HTML_REPORT_PATH)}")