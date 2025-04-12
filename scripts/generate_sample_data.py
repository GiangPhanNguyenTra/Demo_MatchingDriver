import numpy as np
import pandas as pd
import math
import time
from concurrent.futures import ThreadPoolExecutor
from scipy.spatial import KDTree
import threading

def haversine(lat1, lon1, lat2, lon2):
    """Calculate distance in kilometers between two lat/lon pairs."""
    R = 6371  # Earth radius in km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) *
         math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    c = 2 * math.asin(math.sqrt(a))
    return R * c

def generate_drivers(num_drivers, lat_min=10.70, lat_max=10.90, lon_min=106.62, lon_max=106.82):
    drivers = []
    while len(drivers) < num_drivers:
        lat = np.random.uniform(lat_min, lat_max)
        lon = np.random.uniform(lon_min, lon_max)
        drivers.append({'id_driver': len(drivers) + 1, 'lat_driver': lat, 'lon_driver': lon})
    return pd.DataFrame(drivers)

def generate_trips(num_trips, lat_min=10.70, lat_max=10.90, lon_min=106.62, lon_max=106.82):
    trips = []
    while len(trips) < num_trips:
        lat_start = np.random.uniform(lat_min, lat_max)
        lon_start = np.random.uniform(lon_min, lon_max)
        for _ in range(100):  # tối đa 100 lần thử
            lat_end = np.random.uniform(lat_min, lat_max)
            lon_end = np.random.uniform(lon_min, lon_max)
            distance = haversine(lat_start, lon_start, lat_end, lon_end)
            if 4 <= distance <= 50:
                trips.append({
                    'id_trip': len(trips) + 1,
                    'id_customer': len(trips) + 1,
                    'lat_start': lat_start,
                    'lon_start': lon_start,
                    'lat_end': lat_end,
                    'lon_end': lon_end
                })
                break
    return pd.DataFrame(trips)

def find_nearest_drivers(trip, drivers_df, num_drivers=5):
    """Find the nearest drivers for a given trip using KDTree."""
    drivers_locations = drivers_df[['lat_driver', 'lon_driver']].values
    trip_location = np.array([trip['lat_start'], trip['lon_start']])
    
    # Create KDTree
    tree = KDTree(drivers_locations)
    
    # Query the nearest drivers
    k = min(num_drivers, len(drivers_locations))  # Đảm bảo k không vượt quá số lượng tài xế
    dist, idx = tree.query(trip_location, k=k)
    
    nearest_drivers = []
    for i in range(len(idx)):
        nearest_drivers.append((drivers_df.iloc[idx[i]]['id_driver'], dist[i]))
    
    return nearest_drivers

def process_trip(trip, drivers_df, assigned_drivers_lock, assigned_drivers, start_time):
    """Process a single trip to find the nearest available driver."""
    nearest_drivers = find_nearest_drivers(trip, drivers_df)
    
    for driver_id, _ in nearest_drivers:
        with assigned_drivers_lock:  # Sử dụng lock để đồng bộ hóa truy cập
            if driver_id not in assigned_drivers:
                assigned_drivers.add(driver_id)
                
                end_time = time.time()  # Record the end time
                response_time = end_time - start_time
                
                # Calculate the distance between trip start and end
                trip_distance = haversine(trip['lat_start'], trip['lon_start'], trip['lat_end'], trip['lon_end'])
                
                # Find the selected driver
                selected_driver = drivers_df[drivers_df['id_driver'] == driver_id].iloc[0]
                
                # Calculate the distance between the customer and the selected driver
                driver_customer_distance = haversine(trip['lat_start'], trip['lon_start'], selected_driver['lat_driver'], selected_driver['lon_driver'])
                
                return {
                    'id_customer': trip['id_customer'],
                    'id_driver': driver_id,
                    'distance': round(trip_distance, 3),
                    'driver_customer_distance': round(driver_customer_distance, 3),
                    'response_time': round(response_time, 6),
                    'status': 'success' if response_time <= 3 else 'fail'
                }
    
    # If no driver is available
    end_time = time.time()  # Record the end time
    response_time = end_time - start_time
    return {
        'id_customer': trip['id_customer'],
        'id_driver': None,
        'distance': None,
        'driver_customer_distance': None,
        'response_time': round(response_time, 6),
        'status': 'fail'
    }

def dispatch_drivers(trips_df, drivers_df):
    """Dispatch drivers to trips using a concurrent approach."""
    start_time = time.time()
    assigned_drivers = set()
    assigned_drivers_lock = threading.Lock()  # Tạo lock để đồng bộ hóa truy cập
    results = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust max_workers as needed
        futures = [executor.submit(process_trip, trip, drivers_df, assigned_drivers_lock, assigned_drivers, start_time) 
                   for index, trip in trips_df.iterrows()]
        
        for future in futures:
            results.append(future.result())

    total_time = time.time() - start_time
    print(f"Total dispatch time: {total_time:.4f} seconds")
    
    return pd.DataFrame(results)

def save_to_csv(drivers_df, trips_df, results_df):
    drivers_df.to_csv('data/drivers.csv', index=False)
    trips_df.to_csv('data/trips.csv', index=False)
    results_df.to_csv('data/dispatch_results.csv', index=False)

if __name__ == '__main__':
    num_drivers = 1200
    num_trips =  1000
    drivers_df = generate_drivers(num_drivers)
    trips_df = generate_trips(num_trips)
    
    # Perform dispatch using the dispatch_drivers function
    results_df = dispatch_drivers(trips_df, drivers_df)

    # Save the results to CSV
    save_to_csv(drivers_df, trips_df, results_df)
