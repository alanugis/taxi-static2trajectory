import json
from datetime import datetime, timedelta
import os

def parse_timestamp(timestamp):
    """Convert timestamp string to datetime object."""
    return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")

def generate_interval_files(input_file, interval_minutes=60):
    """
    Process the routes JSON file and generate interval-based JSON files.
    Each interval file will contain:
    1. Points from routes that end exactly at the interval time (interval_end)
    2. End points of routes that completed during the previous interval (previous_end)
    3. Current positions of ongoing routes at the interval time (ongoing)
    
    Args:
        input_file (str): Path to the input JSON file containing routes
        interval_minutes (int): Time interval in minutes (default: 15)
    """
    # Create output directory if it doesn't exist
    output_dir = "time_intervals"
    os.makedirs(output_dir, exist_ok=True)
    
    # Read the input JSON file
    print(f"Reading routes from {input_file}...")
    with open(input_file, 'r') as f:
        routes = json.load(f)
    
    # Find the overall time range
    all_timestamps = []
    for feature in routes['features']:
        timestamps = [parse_timestamp(t) for t in feature['properties']['times']]
        all_timestamps.extend(timestamps)
    
    start_time = min(all_timestamps)
    end_time = max(all_timestamps)
    
    # Round start_time down to nearest interval
    minutes = start_time.minute
    rounded_minutes = (minutes // interval_minutes) * interval_minutes
    start_time = start_time.replace(minute=rounded_minutes, second=0, microsecond=0)
    
    # Generate time intervals
    current_time = start_time
    interval_delta = timedelta(minutes=interval_minutes)
    
    print(f"Generating interval files from {start_time} to {end_time}")
    
    while current_time <= end_time:
        interval_points = []
        
        # Check each route
        for feature in routes['features']:
            coordinates = feature['geometry']['coordinates']
            timestamps = [parse_timestamp(t) for t in feature['properties']['times']]
            trip_id = feature['properties']['tripId']
            
            # Get first and last timestamps of the route
            first_timestamp = timestamps[0]
            last_timestamp = timestamps[-1]
            last_idx = len(timestamps) - 1

            # 1. Check if route ends exactly at the interval time
            for point_idx, (point_time, coord) in enumerate(zip(timestamps, coordinates)):
                time_diff = abs(point_time - current_time)
                
                # Only consider end points that are very close to the interval time
                if time_diff < timedelta(minutes=1) and point_idx == last_idx:
                    end_point = {
                        'route_id': trip_id,
                        'longitude': coord[0],
                        'latitude': coord[1],
                        'timestamp': point_time.strftime("%Y-%m-%d %H:%M:%S"),
                        'point_type': 'interval_end'  # Mark as end point at interval
                    }
                    interval_points.append(end_point)
                    break

            # 2. Check if route ended in the previous interval
            interval_start = current_time - interval_delta

            # 3. For ongoing routes at the current interval time
            if first_timestamp < current_time < last_timestamp:
                # Find the point closest to current_time
                closest_point = None
                min_time_diff = timedelta(minutes=interval_minutes)

                for point_idx, (point_time, coord) in enumerate(zip(timestamps, coordinates)):
                    time_diff = abs(point_time - current_time)
                    if time_diff < min_time_diff:
                        min_time_diff = time_diff
                        closest_point = {
                            'route_id': trip_id,
                            'longitude': coord[0],
                            'latitude': coord[1],
                            'timestamp': point_time.strftime("%Y-%m-%d %H:%M:%S"),
                            'point_type': 'ongoing'  # Mark as ongoing route point
                        }
                
                if closest_point:
                    interval_points.append(closest_point)
            
            # If route ended in previous interval (but not exactly at the interval time)
            if interval_start < last_timestamp < current_time - timedelta(minutes=1):
                end_point = {
                    'route_id': trip_id,
                    'longitude': coordinates[-1][0],
                    'latitude': coordinates[-1][1],
                    'timestamp': last_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    'point_type': 'previous_end'  # Mark as end point from previous interval
                }
                interval_points.append(end_point)
        
        if interval_points:
            # Generate filename based on the interval start time
            filename = f"points_{current_time.strftime('%Y%m%d_%H%M')}.json"
            output_path = os.path.join(output_dir, filename)
            
            # Create a GeoJSON feature collection
            feature_collection = {
                'type': 'FeatureCollection',
                'interval': {
                    'time': current_time.strftime("%Y-%m-%d %H:%M:%S"),
                    'interval_start': (current_time - interval_delta).strftime("%Y-%m-%d %H:%M:%S"),
                    'interval_end': current_time.strftime("%Y-%m-%d %H:%M:%S")
                },
                'features': [{
                    'type': 'Feature',
                    'properties': {
                        'route_id': point['route_id'],
                        'timestamp': point['timestamp'],
                        'point_type': point['point_type']  # 'interval' or 'end'
                    },
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [point['longitude'], point['latitude']]
                    }
                } for point in interval_points]
            }
            
            with open(output_path, 'w') as f:
                json.dump(feature_collection, f, indent=2)
            
            print(f"Generated {filename} with {len(interval_points)} points")
        
        current_time += interval_delta

if __name__ == "__main__":
    #---------------------------------------------------
    input_file = "SF_routes_20230701.json"
    #---------------------------------------------------
    generate_interval_files(input_file)
