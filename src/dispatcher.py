# src/dispatcher.py
import numpy as np
import pandas as pd
import math
import time
import os
from concurrent.futures import ThreadPoolExecutor
from scipy.spatial import KDTree
import threading

# --- Hàm tính khoảng cách Haversine ---
def haversine(lat1, lon1, lat2, lon2):
    """Calculate distance in kilometers between two lat/lon pairs."""
    if any(pd.isna([lat1, lon1, lat2, lon2])): # Kiểm tra NaN
        return float('inf') # Trả về vô cùng nếu có NaN để không bị chọn
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) *
         math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    # Kiểm tra giá trị đầu vào của asin để tránh lỗi domain
    a = max(0, min(1, a))
    c = 2 * math.asin(math.sqrt(a))
    return R * c

# --- Hàm tìm tài xế gần nhất bằng KDTree ---
def find_nearest_drivers_kdtree(trip_location, driver_tree, drivers_df, k=10, max_dist_km=100.0):
    """Find the nearest drivers using KDTree within max_dist_km."""
    num_drivers_total = len(drivers_df)
    query_k = min(k * 2, num_drivers_total) # Query nhiều hơn một chút
    if query_k == 0:
        return []

    # KDTree sử dụng tọa độ gốc (lat/lon)
    try:
        distances_kdt, indices_kdt = driver_tree.query(trip_location, k=query_k)
         # Xử lý trường hợp query trả về số lượng ít hơn k hoặc chỉ 1 kết quả
        if num_drivers_total == 1 or query_k == 1 : # Nếu chỉ có 1 tài xế hoặc chỉ query 1
             if isinstance(indices_kdt, (int, np.int_)): # Nếu trả về 1 index
                indices_kdt = [indices_kdt]
             # distances_kdt có thể là float, cần kiểm tra và bọc list nếu cần
             if isinstance(distances_kdt, (float, np.float_)):
                distances_kdt = [distances_kdt]
        elif isinstance(indices_kdt, np.int_): # numpy scalar int
             indices_kdt = [indices_kdt.item()]
             distances_kdt = [distances_kdt.item()] # Giả định distances cũng là scalar

    except ValueError as e:
        print(f"Warning: KDTree query failed for {trip_location} (k={query_k}, total_drivers={num_drivers_total}). Error: {e}. Returning empty list.")
        return []
    except Exception as e:
        print(f"Unexpected error during KDTree query for location {trip_location}: {e}")
        return []


    nearest_drivers = []
    trip_lat, trip_lon = trip_location

    valid_indices = [idx for idx in indices_kdt if idx < num_drivers_total] # Lọc chỉ số hợp lệ

    for idx in valid_indices:
        driver_row = drivers_df.iloc[idx]
        driver_lat, driver_lon = driver_row['lat_driver'], driver_row['lon_driver']

        actual_distance_km = haversine(trip_lat, trip_lon, driver_lat, driver_lon)

        if actual_distance_km <= max_dist_km:
            nearest_drivers.append({
                'id_driver': driver_row['id_driver'],
                'distance_km': actual_distance_km,
                'lat': driver_lat,
                'lon': driver_lon
            })

    nearest_drivers.sort(key=lambda x: x['distance_km'])
    return nearest_drivers[:k]

# --- Hàm xử lý một chuyến đi ---
def process_trip(trip, drivers_df, driver_kdtree, assigned_drivers_lock, assigned_drivers_set, k_nearest=100, max_driver_dist_km=100.0):
    """
    Process a single trip: find nearest available driver using KDTree and lock.
    Returns a dictionary with matching results.
    """
    start_process_time = time.perf_counter()
    trip_id = trip.get('id_trip', trip.get('id_customer', -1)) # Lấy id để log lỗi

    # Kiểm tra dữ liệu trip đầu vào
    required_cols = ['lat_start', 'lon_start', 'lat_end', 'lon_end', 'id_customer']
    if any(pd.isna(trip.get(col)) for col in required_cols):
        end_process_time = time.perf_counter()
        return {
            'id_customer': trip.get('id_customer', -1), 'id_driver': None,
            'distance_km': None, 'driver_customer_distance_km': None,
            'response_time_ms': round((end_process_time - start_process_time) * 1000, 3),
            'status': 'fail (invalid trip data)'
        }

    trip_location = (trip['lat_start'], trip['lon_start'])

    try:
        nearest_drivers_info = find_nearest_drivers_kdtree(
            trip_location,
            driver_kdtree,
            drivers_df,
            k=k_nearest,
            max_dist_km=max_driver_dist_km
        )
    except Exception as e:
        print(f"Error finding drivers for trip {trip_id}: {e}")
        nearest_drivers_info = [] # Xử lý như không tìm thấy tài xế


    assigned_driver_info = None
    status = 'fail (init)'
    response_time_ms = 0

    if nearest_drivers_info:
        with assigned_drivers_lock:
            for driver_info in nearest_drivers_info:
                driver_id = driver_info['id_driver']
                if driver_id not in assigned_drivers_set:
                    assigned_drivers_set.add(driver_id)
                    assigned_driver_info = driver_info
                    status = 'success (assigned)'
                    break
    # Lock released

    end_process_time = time.perf_counter()
    response_time_ms = (end_process_time - start_process_time) * 1000

    # Tính khoảng cách chuyến đi
    trip_distance_km = haversine(trip['lat_start'], trip['lon_start'], trip['lat_end'], trip['lon_end'])
    if trip_distance_km == float('inf'): # Nếu khoảng cách là vô cùng do dữ liệu xấu
         trip_distance_km = None # Đặt là None

    result_data = {
        'id_customer': trip['id_customer'],
        'id_driver': None,
        'distance_km': round(trip_distance_km, 3) if trip_distance_km is not None else None,
        'driver_customer_distance_km': None,
        'response_time_ms': round(response_time_ms, 3),
        'status': 'fail' # Mặc định
    }

    if status == 'success (assigned)' and assigned_driver_info:
        result_data['id_driver'] = assigned_driver_info['id_driver']
        result_data['driver_customer_distance_km'] = round(assigned_driver_info['distance_km'], 3)
        if response_time_ms > 5000:
            result_data['status'] = 'fail (timeout)'
        else:
            result_data['status'] = 'success'
    else: # status vẫn là 'fail (init)' hoặc không tìm được assigned_driver_info
        if not nearest_drivers_info:
            result_data['status'] = 'fail (no driver nearby)'
        else:
            result_data['status'] = 'fail (all drivers busy)'

    return result_data

# --- Hàm điều phối chính ---
def dispatch_drivers_locked(trips_df, drivers_df, max_workers=None):
    """
    Dispatch drivers to trips using KDTree, ThreadPoolExecutor, and Lock.
    Assumes trips_df and drivers_df are pre-loaded DataFrames.
    """
    if trips_df.empty:
        print("No trips data loaded or trips file is empty.")
        return pd.DataFrame()
    if drivers_df.empty:
        print("No drivers data loaded or drivers file is empty. All trips will fail.")
        # Trả về kết quả fail nhanh chóng
        results = []
        start_fail_time = time.perf_counter()
        for index, trip in trips_df.iterrows():
             trip_dist = None
             if pd.notna(trip.get('lat_start')) and pd.notna(trip.get('lon_start')) and pd.notna(trip.get('lat_end')) and pd.notna(trip.get('lon_end')):
                trip_dist = round(haversine(trip['lat_start'], trip['lon_start'], trip['lat_end'], trip['lon_end']), 3)
                if trip_dist == float('inf'): trip_dist = None

             end_fail_time = time.perf_counter()
             results.append({
                 'id_customer': trip.get('id_customer', -1), 'id_driver': None,
                 'distance_km': trip_dist, 'driver_customer_distance_km': None,
                 'response_time_ms': round((end_fail_time - start_fail_time) * 1000, 3),
                 'status': 'fail (no drivers available)'
             })
        return pd.DataFrame(results)

    print(f"Starting dispatch for {len(trips_df)} trips with {len(drivers_df)} drivers...")
    dispatch_start_time = time.time()

    # --- Chuẩn bị ---
    required_driver_cols = ['lat_driver', 'lon_driver', 'id_driver']
    if not all(col in drivers_df.columns for col in required_driver_cols):
        print(f"Error: Drivers DataFrame missing required columns: {required_driver_cols}")
        return pd.DataFrame()
    required_trip_cols = ['lat_start', 'lon_start', 'lat_end', 'lon_end', 'id_customer']
    if not all(col in trips_df.columns for col in required_trip_cols):
         print(f"Error: Trips DataFrame missing required columns: {required_trip_cols}")
         return pd.DataFrame()

    # Loại bỏ tài xế có tọa độ NaN trước khi tạo KDTree
    drivers_df_clean = drivers_df.dropna(subset=['lat_driver', 'lon_driver']).copy()
    if len(drivers_df_clean) != len(drivers_df):
         print(f"Warning: Removed {len(drivers_df) - len(drivers_df_clean)} drivers with invalid coordinates.")
    if drivers_df_clean.empty:
        print("Error: No valid driver coordinates found after cleaning. Cannot create KDTree.")
        # Trả về lỗi tương tự như không có tài xế
        return dispatch_drivers_locked(trips_df, drivers_df_clean) # Gọi lại với df rỗng để tạo fail results


    driver_locations = drivers_df_clean[['lat_driver', 'lon_driver']].values
    try:
        driver_kdtree = KDTree(driver_locations)
        print("KDTree for drivers created.")
    except Exception as e:
        print(f"Error creating KDTree: {e}. Ensure driver locations are valid numbers.")
        return pd.DataFrame() # Trả về df rỗng nếu không tạo được tree

    # Khởi tạo Lock và Set
    assigned_drivers_set = set()
    assigned_drivers_lock = threading.Lock()

    results = []
    if max_workers is None:
        max_workers = min(32, os.cpu_count() + 4) # Giới hạn worker, giá trị này có thể tinh chỉnh
        print(f"Using default max_workers: {max_workers}")

    # --- Thực thi song song ---
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                process_trip,
                row, # Truyền Series của trip
                drivers_df_clean, # Sử dụng df đã làm sạch
                driver_kdtree,
                assigned_drivers_lock,
                assigned_drivers_set,
                k_nearest=100, # Số lượng tài xế gần nhất để tìm kiếm
                max_driver_dist_km=100.0
            ): index # Lưu index hoặc id_trip để debug nếu cần
            for index, row in trips_df.iterrows()
        }

        processed_count = 0
        total_trips = len(futures)
        # Sử dụng as_completed để lấy kết quả ngay khi có thể
        from concurrent.futures import as_completed
        for future in as_completed(futures):
            # index = futures[future] # Lấy lại index/id nếu cần
            try:
                result = future.result()
                results.append(result)
                processed_count += 1
                if processed_count % (total_trips // 20 or 1) == 0 or processed_count == total_trips:
                     print(f"  Processed {processed_count}/{total_trips} trips...")
            except Exception as e:
                # Lỗi xảy ra bên trong process_trip đã được xử lý phần nào,
                # lỗi ở đây thường là do future bị cancel hoặc lỗi executor
                print(f"Error retrieving result from future: {e}")
                results.append({
                    'id_customer': -1, 'id_driver': None, 'distance_km': None,
                    'driver_customer_distance_km': None, 'response_time_ms': -1,
                    'status': 'fail (future error)'
                })


    dispatch_end_time = time.time()
    total_time = dispatch_end_time - dispatch_start_time
    print(f"\nFinished dispatching {len(results)} trips.")
    print(f"Total dispatch wall time: {total_time:.4f} seconds")

    # --- Phân tích kết quả ---
    # (Phần này giữ nguyên như code trước)
    if not results:
        print("No results were generated during dispatch.")
        return pd.DataFrame()

    results_df = pd.DataFrame(results)
    # ... (phần summary và tính toán thống kê giữ nguyên) ...
    success_count = results_df[results_df['status'] == 'success'].shape[0]
    fail_timeout_count = results_df[results_df['status'] == 'fail (timeout)'].shape[0]
    fail_busy_count = results_df[results_df['status'] == 'fail (all drivers busy)'].shape[0]
    fail_nearby_count = results_df[results_df['status'] == 'fail (no driver nearby)'].shape[0]
    fail_invalid_trip = results_df[results_df['status'] == 'fail (invalid trip data)'].shape[0]
    fail_no_drivers = results_df[results_df['status'] == 'fail (no drivers available)'].shape[0]
    fail_other_count = results_df[~results_df['status'].isin([
        'success', 'fail (timeout)', 'fail (all drivers busy)',
        'fail (no driver nearby)', 'fail (invalid trip data)',
        'fail (no drivers available)'
    ])].shape[0]


    print("\n--- Dispatch Summary ---")
    print(f"Total Trips Processed: {len(results_df)}")
    print(f"Successful Matches: {success_count}")
    print(f"Failed (Timeout > 5s): {fail_timeout_count}")
    print(f"Failed (All Drivers Busy): {fail_busy_count}")
    print(f"Failed (No Driver Nearby): {fail_nearby_count}")
    print(f"Failed (Invalid Trip Data): {fail_invalid_trip}")
    if fail_no_drivers > 0: # Chỉ hiển thị nếu có lỗi này
         print(f"Failed (No Drivers Available/Loaded): {fail_no_drivers}")
    print(f"Failed (Other Reasons): {fail_other_count}")


    expected_fails = max(0, len(trips_df) - len(drivers_df_clean)) # So sánh với số tài xế hợp lệ
    # Tính tổng số fail thực tế từ các loại fail đã biết
    actual_fails = fail_timeout_count + fail_busy_count + fail_nearby_count + fail_invalid_trip + fail_no_drivers + fail_other_count
    print(f"Expected minimum failures due to driver shortage (valid drivers): {expected_fails}")
    print(f"Actual total failures recorded: {actual_fails}")
    # Lưu ý: actual_fails có thể lớn hơn expected_fails do các lý do khác (timeout, busy, nearby)

    if not results_df.empty:
        valid_times = results_df['response_time_ms'].dropna()
        valid_times = valid_times[valid_times >= 0]
        if not valid_times.empty:
            avg_resp_time = valid_times.mean()
            max_resp_time = valid_times.max()
            p95_resp_time = valid_times.quantile(0.95)
            median_resp_time = valid_times.median()
            print(f"\nResponse Time Stats (ms) for all processed trips:")
            print(f"  Average: {avg_resp_time:.2f}")
            print(f"  Median: {median_resp_time:.2f}")
            print(f"  95th Percentile: {p95_resp_time:.2f}")
            print(f"  Max: {max_resp_time:.2f}")

            # Thống kê riêng cho các chuyến thành công
            success_times = results_df[results_df['status'] == 'success']['response_time_ms'].dropna()
            if not success_times.empty:
                 avg_success_time = success_times.mean()
                 median_success_time = success_times.median()
                 p95_success_time = success_times.quantile(0.95)
                 max_success_time = success_times.max()
                 print(f"\nResponse Time Stats (ms) for SUCCESSFUL trips only:")
                 print(f"  Average: {avg_success_time:.2f}")
                 print(f"  Median: {median_success_time:.2f}")
                 print(f"  95th Percentile: {p95_success_time:.2f}")
                 print(f"  Max: {max_success_time:.2f}")

        else:
            print("\nNo valid response times recorded.")
    else:
        print("\nResults DataFrame is empty, cannot calculate stats.")


    return results_df