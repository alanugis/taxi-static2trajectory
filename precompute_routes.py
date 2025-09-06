import json
import pandas as pd
import requests
import time
from datetime import datetime
import asyncio
import aiohttp
import os
from aiohttp import ClientTimeout

# Constants for retry settings
MAX_RETRIES = 5
INITIAL_WAIT = 1  # seconds
MAX_WAIT = 120  # seconds
CONCURRENT_REQUESTS = 10  # Reduced from default
CHUNK_SIZE = 1000  # Save progress every 1000 trips

async def get_osrm_route(session, start_lng, start_lat, end_lng, end_lat, retry_count=0):
    """Get route from OSRM asynchronously with retries"""
    url = f"https://router.project-osrm.org/route/v1/driving/{start_lng},{start_lat};{end_lng},{end_lat}?overview=full&geometries=geojson"
    
    # Exponential backoff for retries
    wait_time = min(INITIAL_WAIT * (2 ** retry_count), MAX_WAIT)
    
    try:
        timeout = ClientTimeout(total=120)  # 30 seconds timeout
        async with session.get(url, timeout=timeout) as response:
            if response.status == 429:  # Rate limited
                if retry_count < MAX_RETRIES:
                    print(f"Rate limited, waiting {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                    return await get_osrm_route(session, start_lng, start_lat, end_lng, end_lat, retry_count + 1)
                return None

            data = await response.json()
            if data["code"] == "Ok" and data["routes"] and data["routes"][0]["geometry"]["coordinates"]:
                route = data["routes"][0]
                return {
                    "coordinates": route["geometry"]["coordinates"],
                    "duration": route["duration"],  # Duration in seconds
                    "distance": route["distance"],  # Distance in meters
                    "average_speed": route["distance"] / route["duration"] if route["duration"] > 0 else 0  # Speed in m/s
                }
            
            # If route not found but response was OK, return direct line with zero duration/distance
            return {
                "coordinates": [[start_lng, start_lat], [end_lng, end_lat]],
                "duration": 0,
                "distance": 0,
                "average_speed": 0
            }
            
    except asyncio.TimeoutError:
        print(f"Timeout error for route {start_lng},{start_lat} to {end_lng},{end_lat}")
        if retry_count < MAX_RETRIES:
            await asyncio.sleep(wait_time)
            return await get_osrm_route(session, start_lng, start_lat, end_lng, end_lat, retry_count + 1)
        return [[start_lng, start_lat], [end_lng, end_lat]]  # Fallback to direct line
        
    except Exception as e:
        print(f"Error fetching route: {e}")
        if retry_count < MAX_RETRIES:
            await asyncio.sleep(wait_time)
            return await get_osrm_route(session, start_lng, start_lat, end_lng, end_lat, retry_count + 1)
        return [[start_lng, start_lat], [end_lng, end_lat]]  # Fallback to direct line

async def process_chunk(chunk_data, session, chunk_num, total_chunks):
    """Process a chunk of trips"""
    features = []
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    async def process_trip(row, trip_num, total_trips):
        async with semaphore:  # Limit concurrent requests
            if pd.isna(row["pickup_location_latitude"]) or pd.isna(row["pickup_location_longitude"]):
                return None

            pickup_lat = float(row["pickup_location_latitude"])
            pickup_lng = float(row["pickup_location_longitude"])
            dropoff_lat = float(row["dropoff_location_latitude"]) if pd.notna(row["dropoff_location_latitude"]) else pickup_lat
            dropoff_lng = float(row["dropoff_location_longitude"]) if pd.notna(row["dropoff_location_longitude"]) else pickup_lng
            
            route_data = await get_osrm_route(
                session, pickup_lng, pickup_lat, dropoff_lng, dropoff_lat
            )
            
            if route_data:
                start_time = pd.to_datetime(row["start_time_local"])
                end_time = pd.to_datetime(row["end_time_local"])
                duration_seconds = (end_time - start_time).total_seconds()
                
                num_points = len(route_data["coordinates"])
                times = []
                for i in range(num_points):
                    point_time = start_time + pd.Timedelta(seconds=duration_seconds * (i / (num_points - 1)))
                    times.append(point_time.strftime("%Y-%m-%dT%H:%M:%S"))

                feature = {
                    "type": "Feature",
                    "properties": {
                        "tripId": row["trip_ID"],
                        "times": times,
                        "duration": route_data["duration"],  # Total route time in seconds
                        "distance": route_data["distance"],  # Total distance in meters
                        "average_speed": route_data["average_speed"],  # Average speed in m/s
                        "popup": f"Trip {row['trip_ID']}<br>Start: {start_time}<br>End: {end_time}<br>Distance: {route_data['distance']:.0f}m<br>Duration: {route_data['duration']:.0f}s<br>Avg Speed: {route_data['average_speed']:.1f}m/s"
                    },
                    "geometry": {
                        "type": "LineString",
                        "coordinates": route_data["coordinates"]
                    }
                }
                
                # Print progress
                if trip_num % 10 == 0:  # Show progress every 10 trips
                    print(f"\rProgress: Chunk {chunk_num}/{total_chunks}, Trip {trip_num}/{total_trips}", end="")
                
                return feature
            return None

    # Process trips in parallel with controlled concurrency
    total_trips = len(chunk_data)
    tasks = [process_trip(row, idx, total_trips) for idx, (_, row) in enumerate(chunk_data.iterrows(), 1)]
    results = await asyncio.gather(*tasks)
    
    # Filter out None results and extend features list
    features.extend([f for f in results if f is not None])
    return features

async def process_trips(trips_data):
    """Process all trips asynchronously with chunking and progress saving"""
    features = []
    
    # Load existing progress if any
    if os.path.exists("precomputed_routes_partial.json"):
        with open("precomputed_routes_partial.json", "r") as f:
            existing_data = json.load(f)
            features = existing_data.get("features", [])
            processed_ids = set(f["properties"]["tripId"] for f in features)
            # Filter out already processed trips
            trips_data = trips_data[~trips_data["trip_ID"].isin(processed_ids)]
            print(f"Loaded {len(features)} existing features, {len(trips_data)} trips remaining")

    # Configure aiohttp session with longer timeouts and keep-alive
    timeout = ClientTimeout(total=60)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS, ttl_dns_cache=300)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Process in chunks
        chunks = [trips_data[i:i + CHUNK_SIZE] for i in range(0, len(trips_data), CHUNK_SIZE)]
        total_chunks = len(chunks)
        
        print(f"Processing {len(trips_data)} trips in {total_chunks} chunks")
        
        for i, chunk in enumerate(chunks, 1):
            chunk_features = await process_chunk(chunk, session, i, total_chunks)
            features.extend(chunk_features)
            
            # Save progress after each chunk
            partial_geojson = {
                "type": "FeatureCollection",
                "features": features
            }
            with open("precomputed_routes_partial.json", "w") as f:
                json.dump(partial_geojson, f)
            print(f"\nSaved progress: {len(features)} features processed")
            
            # Add delay between chunks to avoid overwhelming the server
            if i < total_chunks:
                await asyncio.sleep(2)
    
    return features

async def main():
    # Load the trip data
    print("Loading trip data...")
    # -----------------------------------------------------------------------------------
    df = pd.read_csv("SF_TaxiTrips_20230701.csv")
    # -----------------------------------------------------------------------------------

    print(f"Processing {len(df)} trips...")
    features = await process_trips(df)
    
    # Save the final routes
    print("Saving precomputed routes...")
    geojson_data = {
        "type": "FeatureCollection",
        "features": features
    }
    
    with open("precomputed_routes.json", "w") as f:
        json.dump(geojson_data, f)
    
    # Clean up partial file if it exists
    if os.path.exists("precomputed_routes_partial.json"):
        os.remove("precomputed_routes_partial.json")
    
    print(f"Done! Saved {len(features)} routes to precomputed_routes.json")

if __name__ == "__main__":
    asyncio.run(main())
