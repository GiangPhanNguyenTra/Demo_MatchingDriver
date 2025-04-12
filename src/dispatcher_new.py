# dispatcher_new.py
import pandas as pd
import numpy as np
import math
import time
import os
import threading
from scipy.spatial import KDTree

def haversine(lat1, lon1, lat2, lon2):
    if any(pd.isna([lat1, lon1, lat2, lon2])):
        return float('inf')
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) *
         math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    a = max(0, min(1, a))
    c = 2 * math.asin(math.sqrt(a))
    return R * c

class Dispatcher:
    def __init__(self, driver_csv_path, k_nearest=400, max_driver_dist_km=30.0):
        self.k_nearest = k_nearest
        self.max_driver_dist_km = max_driver_dist_km
        self.driver_csv_path = driver_csv_path
        self.drivers_df = pd.DataFrame()
        self.drivers_df_clean = pd.DataFrame()
        self.driver_kdtree = None
        self.assigned_drivers_set = set()
        self.assigned_drivers_lock = threading.Lock()
        self._load_and_prepare_drivers()

    def _load_and_prepare_drivers(self):
        if not os.path.exists(self.driver_csv_path):
            print(f"Error: Driver file not found at {self.driver_csv_path}")
            # Optional: Create a dummy file for basic testing if needed
            # dummy_df = pd.DataFrame({'id_driver': [1], 'lat_driver': [10.0], 'lon_driver': [106.0]})
            # os.makedirs(os.path.dirname(self.driver_csv_path), exist_ok=True)
            # dummy_df.to_csv(self.driver_csv_path, index=False)
            # print("Created a dummy driver file.")
            # self.drivers_df = dummy_df # Use dummy if created
            # Or raise an error:
            raise FileNotFoundError(f"Driver file not found: {self.driver_csv_path}")


        print(f"Loading drivers from {self.driver_csv_path}...")
        self.drivers_df = pd.read_csv(self.driver_csv_path)
        print(f"Loaded {len(self.drivers_df)} drivers initially.")

        required_cols = ['id_driver', 'lat_driver', 'lon_driver']
        if not all(col in self.drivers_df.columns for col in required_cols):
            raise ValueError(f"Driver CSV must contain columns: {required_cols}")

        self.drivers_df_clean = self.drivers_df.dropna(subset=['lat_driver', 'lon_driver']).copy()
        num_removed = len(self.drivers_df) - len(self.drivers_df_clean)
        if num_removed > 0:
            print(f"Removed {num_removed} drivers with invalid coordinates.")

        if self.drivers_df_clean.empty:
            print("Error: No valid driver data after cleaning. Dispatcher will not work.")
            self.driver_kdtree = None # Ensure KDTree is None
        else:
            print(f"Building KDTree for {len(self.drivers_df_clean)} valid drivers...")
            driver_locations = self.drivers_df_clean[['lat_driver', 'lon_driver']].values
            try:
                self.driver_kdtree = KDTree(driver_locations)
                print("KDTree built successfully.")
            except Exception as e:
                print(f"Error creating KDTree: {e}. Dispatcher may not function correctly.")
                self.driver_kdtree = None # Set to None on failure

    def _find_nearest_drivers_kdtree(self, trip_location, k, max_dist_km):
        if self.driver_kdtree is None or len(self.drivers_df_clean) == 0:
            return []

        num_drivers_total = len(self.drivers_df_clean)
        query_k = min(k * 2, num_drivers_total) # Query more initially

        try:
            distances_kdt, indices_kdt = self.driver_kdtree.query(trip_location, k=query_k, workers=-1) # Use all available cores for query

            if num_drivers_total == 1 or query_k == 1:
                if isinstance(indices_kdt, (int, np.int_)):
                    indices_kdt = [indices_kdt]
                if isinstance(distances_kdt, (float, np.float_)):
                    distances_kdt = [distances_kdt]
            elif isinstance(indices_kdt, np.int_): # Handle numpy scalar int if only one result found > 1 query
                 indices_kdt = [indices_kdt.item()]
                 distances_kdt = [distances_kdt.item()]
            elif isinstance(indices_kdt, np.ndarray): # Standard case
                 indices_kdt = indices_kdt.tolist() # Convert to list for easier iteration
                 # distances_kdt can usually be used as is with zip

        except ValueError as e:
            print(f"Warning: KDTree query failed for {trip_location} (k={query_k}, total_drivers={num_drivers_total}). Error: {e}")
            return []
        except Exception as e:
            print(f"Unexpected error during KDTree query for {trip_location}: {e}")
            return []

        nearest_drivers = []
        trip_lat, trip_lon = trip_location

        # KDTree distance is Euclidean on lat/lon, recalculate with Haversine and filter
        valid_indices = [idx for idx in indices_kdt if idx < num_drivers_total] # Filter out potential out-of-bounds indices

        # Fetch driver details for valid indices efficiently
        candidate_drivers = self.drivers_df_clean.iloc[valid_indices]

        for index, driver_row in candidate_drivers.iterrows():
            driver_lat, driver_lon = driver_row['lat_driver'], driver_row['lon_driver']
            actual_distance_km = haversine(trip_lat, trip_lon, driver_lat, driver_lon)

            if actual_distance_km <= max_dist_km:
                nearest_drivers.append({
                    'id_driver': driver_row['id_driver'],
                    'distance_km': actual_distance_km,
                    'lat': driver_lat,
                    'lon': driver_lon,
                    # Store original index if needed for debugging
                    # 'original_kdt_index': index
                })

        nearest_drivers.sort(key=lambda x: x['distance_km'])
        return nearest_drivers[:k] # Return only the top k valid drivers


    def dispatch_driver_for_trip(self, trip_data):
        start_process_time = time.perf_counter()

        # --- Input Validation ---
        required_keys = ['id_trip', 'id_customer', 'lat_start', 'lon_start', 'lat_end', 'lon_end']
        if not isinstance(trip_data, dict) or not all(key in trip_data for key in required_keys):
             end_process_time = time.perf_counter()
             return {
                'id_customer': trip_data.get('id_customer', -1) if isinstance(trip_data, dict) else -1,
                'id_driver': None,
                'distance_km': None,
                'driver_customer_distance_km': None,
                'response_time_ms': round((end_process_time - start_process_time) * 1000, 3),
                'status': 'fail (invalid request data)'
             }

        id_customer = trip_data['id_customer']
        lat_start = trip_data['lat_start']
        lon_start = trip_data['lon_start']
        lat_end = trip_data['lat_end']
        lon_end = trip_data['lon_end']

        if any(pd.isna([lat_start, lon_start, lat_end, lon_end])):
            end_process_time = time.perf_counter()
            return {
                'id_customer': id_customer, 'id_driver': None,
                'distance_km': None, 'driver_customer_distance_km': None,
                'response_time_ms': round((end_process_time - start_process_time) * 1000, 3),
                'status': 'fail (invalid trip coordinates)'
            }

        # --- Find Nearest Drivers ---
        trip_location = (lat_start, lon_start)
        nearest_drivers_info = []
        if self.driver_kdtree: # Only search if KDTree is available
            try:
                nearest_drivers_info = self._find_nearest_drivers_kdtree(
                    trip_location,
                    k=self.k_nearest,
                    max_dist_km=self.max_driver_dist_km
                )
            except Exception as e:
                print(f"Error finding drivers for customer {id_customer}: {e}")
                # Proceed as if no drivers were found

        # --- Attempt Assignment ---
        assigned_driver_info = None
        assignment_status = 'fail (init)'

        if not nearest_drivers_info:
             assignment_status = 'fail (no driver nearby)'
        else:
            with self.assigned_drivers_lock:
                for driver_info in nearest_drivers_info:
                    driver_id = driver_info['id_driver']
                    if driver_id not in self.assigned_drivers_set:
                        self.assigned_drivers_set.add(driver_id)
                        assigned_driver_info = driver_info
                        assignment_status = 'success (assigned)'
                        break # Found and assigned a driver
                # If loop finishes without break, all nearby drivers were busy
                if assignment_status == 'fail (init)': # Check if it wasn't updated
                     assignment_status = 'fail (all drivers busy)'
            # Lock released automatically

        # --- Calculate Trip Distance and Response Time ---
        end_process_time = time.perf_counter()
        response_time_ms = (end_process_time - start_process_time) * 1000

        trip_distance_km = haversine(lat_start, lon_start, lat_end, lon_end)
        trip_distance_km = round(trip_distance_km, 3) if trip_distance_km != float('inf') else None

        # --- Format Result ---
        final_status = assignment_status # Start with assignment status
        if final_status == 'success (assigned)' and response_time_ms > 5000:
            final_status = 'fail (timeout)'
            # Note: Driver remains assigned in the set even on timeout in this simple POC
            # A real system might need to roll back the assignment here
        elif final_status == 'success (assigned)':
             final_status = 'success'


        result = {
            'id_customer': id_customer,
            'id_driver': assigned_driver_info['id_driver'] if assigned_driver_info else None,
            'distance_km': trip_distance_km,
            'driver_customer_distance_km': round(assigned_driver_info['distance_km'], 3) if assigned_driver_info else None,
            'response_time_ms': round(response_time_ms, 3),
            'status': final_status
        }

        return result

    def get_assigned_driver_count(self):
         with self.assigned_drivers_lock:
              return len(self.assigned_drivers_set)

    # Optional: Method to reset assigned drivers for testing purposes
    def reset_assignments(self):
         with self.assigned_drivers_lock:
              self.assigned_drivers_set.clear()
         print("Assigned drivers have been reset.")