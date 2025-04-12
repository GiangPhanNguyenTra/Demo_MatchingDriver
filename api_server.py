# api_server.py
import os
import sys
import pandas as pd
import time
import datetime
import threading
import json
from collections import Counter
from flask import Flask, request, jsonify, render_template, Response, stream_with_context
from src.dispatcher_new import Dispatcher

# --- Configuration ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
DRIVER_FILE = os.path.join(DATA_DIR, 'drivers.csv')
TEMPLATE_FOLDER = os.path.join(BASE_DIR, 'templates') # Đường dẫn đến thư mục templates
REPORT_OUTPUT_FILE = os.path.join(DATA_DIR, 'dispatch_report.html') # File output report (nếu cần lưu tĩnh)

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(TEMPLATE_FOLDER, exist_ok=True) # Tạo thư mục templates nếu chưa có

# --- Flask App Initialization ---
# Chỉ định thư mục templates cho Flask
app = Flask(__name__, template_folder=TEMPLATE_FOLDER)

# --- Global State for Live Reporting ---
results_store = [] # Lưu trữ kết quả từ các request dispatch
summary_stats = {} # Lưu trữ thống kê tóm tắt
results_lock = threading.Lock() # Lock để bảo vệ truy cập results_store và summary_stats
last_update_time = time.time()

# --- Global Dispatcher Instance ---
print("Initializing Dispatcher...")
try:
    if not os.path.exists(DRIVER_FILE):
        print(f"WARNING: Driver file not found at {DRIVER_FILE}.")
        print(f"Creating a dummy driver file for testing.")
        dummy_df = pd.DataFrame({
            'id_driver': [f'D{i:04d}' for i in range(1, 1501)], # Tạo nhiều tài xế hơn
            'lat_driver': [10.75 + (i * 0.0001) for i in range(1500)],
            'lon_driver': [106.65 + (i * 0.0001) for i in range(1500)]
        })
        dummy_df.to_csv(DRIVER_FILE, index=False)
        print(f"Dummy driver file created at {DRIVER_FILE}")

    # Điều chỉnh k_nearest và max_driver_dist_km nếu cần
    dispatcher = Dispatcher(driver_csv_path=DRIVER_FILE, k_nearest=500, max_driver_dist_km=50.0)
    print(f"Dispatcher Initialized. Ready to accept requests.")
    print(f"Total valid drivers loaded: {len(dispatcher.drivers_df_clean)}")
    summary_stats['total_drivers_available'] = len(dispatcher.drivers_df_clean)

except FileNotFoundError as e:
    print(f"Initialization failed: {e}")
    sys.exit(1)
except ValueError as e:
    print(f"Initialization failed due to invalid driver data: {e}")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred during dispatcher initialization: {e}")
    sys.exit(1)


# --- Helper function for Summary Calculation ---
def calculate_summary(results_list):
    if not results_list:
        return {
            'total_trips': 0,
            'success_count': 0,
            'total_fails': 0,
            'success_rate': 0,
            'fails_by_reason': {},
            'avg_response_time': None, 'median_response_time': None, 'p95_response_time': None, 'max_response_time': None,
            'avg_success_response_time': None, 'median_success_response_time': None, 'p95_success_response_time': None, 'max_success_response_time': None,
        }

    df = pd.DataFrame(results_list)
    total_trips = len(df)
    success_df = df[df['status'] == 'success']
    success_count = len(success_df)
    total_fails = total_trips - success_count
    success_rate = success_count / total_trips if total_trips > 0 else 0

    fail_reasons = df[df['status'] != 'success']['status'].value_counts().to_dict()

    summary = {
        'total_trips': total_trips,
        'success_count': success_count,
        'total_fails': total_fails,
        'success_rate': success_rate,
        'fails_by_reason': fail_reasons,
        'total_drivers_available': summary_stats.get('total_drivers_available', 0) # Lấy từ global nếu có
    }

    # Response time stats for all valid trips
    valid_times = df['response_time_ms'].dropna()
    valid_times = valid_times[valid_times >= 0]
    if not valid_times.empty:
        summary.update({
            'avg_response_time': valid_times.mean(),
            'median_response_time': valid_times.median(),
            'p95_response_time': valid_times.quantile(0.95),
            'max_response_time': valid_times.max(),
        })
    else:
         summary.update({
            'avg_response_time': None, 'median_response_time': None, 'p95_response_time': None, 'max_response_time': None,
         })


    # Response time stats for successful trips only
    success_times = success_df['response_time_ms'].dropna()
    if not success_times.empty:
         summary.update({
            'avg_success_response_time': success_times.mean(),
            'median_success_response_time': success_times.median(),
            'p95_success_response_time': success_times.quantile(0.95),
            'max_success_response_time': success_times.max(),
         })
    else:
         summary.update({
            'avg_success_response_time': None, 'median_success_response_time': None, 'p95_success_response_time': None, 'max_success_response_time': None,
         })

    return summary

# --- API Endpoints ---

@app.route('/dispatch', methods=['POST'])
def handle_dispatch():
    """
    Handles incoming trip requests, dispatches a driver, stores the result,
    and returns the result as JSON.
    """
    global last_update_time
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400

    trip_data = request.get_json()
    # Gọi dispatcher để xử lý
    result = dispatcher.dispatch_driver_for_trip(trip_data)

    # Lưu kết quả vào global store (thread-safe)
    with results_lock:
        results_store.append(result)
        last_update_time = time.time() # Ghi nhận thời điểm có cập nhật mới

    # Phản hồi cho JMeter (hoặc client gọi API trực tiếp)
    return jsonify(result), 200

@app.route('/report')
def view_report():
    """Serves the initial HTML report page."""
    # Có thể tính toán summary ban đầu ở đây nếu muốn, nhưng để SSE xử lý hết cũng được
    with results_lock:
         current_summary = calculate_summary(results_store)
         current_results = results_store[:] # Tạo bản sao để tránh race condition khi render

    # Render template HTML ban đầu, JS sẽ kết nối tới /stream_results
    return render_template('report_testjmeter_template.html',
                           generation_time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                           summary=current_summary, # Truyền summary ban đầu
                           results=current_results   # Truyền results ban đầu
                           )

@app.route('/stream_results')
def stream_results():
    """Endpoint for Server-Sent Events (SSE) to push updates."""
    def generate():
        last_sent_index = -1
        last_summary_sent = None
        while True:
            new_data_to_send = []
            current_summary = None
            send_update = False

            with results_lock:
                # Kiểm tra xem có kết quả mới không
                if len(results_store) > last_sent_index + 1:
                    new_results = results_store[last_sent_index + 1:]
                    new_data_to_send.extend([{'type': 'result', 'data': res} for res in new_results])
                    last_sent_index = len(results_store) - 1
                    send_update = True

                # Tính toán lại summary nếu có thay đổi hoặc định kỳ
                # (Ở đây tính lại mỗi khi có kết quả mới để đơn giản)
                if send_update:
                    current_summary = calculate_summary(results_store)
                    # Chỉ gửi summary nếu nó khác lần gửi trước
                    if current_summary != last_summary_sent:
                         new_data_to_send.append({'type': 'summary', 'data': current_summary})
                         last_summary_sent = current_summary

            if send_update:
                 # Gửi dữ liệu mới qua SSE
                 # Gửi từng message một để client xử lý dễ hơn
                 for item in new_data_to_send:
                     # Định dạng chuẩn của SSE: "data: <json_string>\n\n"
                     yield f"data: {json.dumps(item)}\n\n"

            # Ngủ một chút để tránh CPU load cao khi không có gì mới
            # Điều chỉnh thời gian sleep nếu cần (ví dụ: 0.2 giây)
            time.sleep(0.1)

    # Trả về Response với mimetype text/event-stream
    return Response(stream_with_context(generate()), mimetype='text/event-stream')

@app.route('/reset', methods=['POST'])
def reset_results():
    """Clears the stored results and resets assigned drivers."""
    global results_store, summary_stats, last_update_time
    with results_lock:
        results_store = []
        summary_stats = {'total_drivers_available': len(dispatcher.drivers_df_clean)} # Giữ lại số tài xế
        last_update_time = time.time()
    # Quan trọng: Reset cả trạng thái tài xế đã gán trong dispatcher
    dispatcher.reset_assignments()
    print("Results and driver assignments have been reset.")
    return jsonify({"message": "Results cleared successfully."}), 200


@app.route('/status', methods=['GET'])
def get_status():
    """Returns basic status including stored results count."""
    with results_lock:
        results_count = len(results_store)
    return jsonify({
        "status": "running",
        "valid_drivers": len(dispatcher.drivers_df_clean) if dispatcher else 0,
        "currently_assigned_drivers": dispatcher.get_assigned_driver_count() if dispatcher else 0,
        "stored_results_count": results_count
    }), 200

# --- Run Flask App ---
if __name__ == '__main__':
    try:
        from waitress import serve
        print("Starting server using Waitress on http://0.0.0.0:5000")
        print("Access the live report at http://localhost:5000/report")
        print("Send POST requests to http://localhost:5000/dispatch")
        print("Send POST requests to http://localhost:5000/reset to clear results before a new test")
        # Tăng backlog và threads nếu cần thiết cho tải cao
        serve(app, host='0.0.0.0', port=5000, threads=500, backlog=2048)
    except ImportError:
        print("Waitress not found. Falling back to Flask's development server.")
        print("Install Waitress: pip install waitress")
        print("Access the live report at http://localhost:5000/report")
        print("Send POST requests to http://localhost:5000/dispatch")
        print("Send POST requests to http://localhost:5000/reset to clear results before a new test")
        app.run(host='0.0.0.0', port=5000, debug=False, threaded=True) # threaded=True quan trọng cho dev server