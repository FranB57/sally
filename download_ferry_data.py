import asyncio
import httpx
import zipfile
import io
import pandas as pd
import json
from pathlib import Path

async def download_and_process_ferry_data():
    """Download GTFS data and process into our nested JSON structure"""

    # Download the GTFS zip
    url = "http://nycferry.connexionz.net/rtt/public/utility/gtfs.aspx"
    print("Downloading ferry GTFS data...")

    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=30.0, follow_redirects=True)
        response.raise_for_status()

    # Extract the files we need
    zip_data = io.BytesIO(response.content)
    gtfs_data = {}

    with zipfile.ZipFile(zip_data, 'r') as zip_ref:
        # Load the key files into pandas DataFrames
        files_to_load = ['stops.txt', 'routes.txt', 'trips.txt', 'stop_times.txt', 'calendar.txt']

        for filename in files_to_load:
            if filename in zip_ref.namelist():
                file_data = zip_ref.read(filename)
                # TODO: Fill in - create a DataFrame from the CSV data
                gtfs_data[filename.replace('.txt', '')] = pd.read_csv(io.StringIO(file_data.decode('utf-8')))

    # Step 1: Start with stops as our base structure
    stops_df = gtfs_data['stops']
    routes_df = gtfs_data['routes']
    trips_df = gtfs_data['trips']
    stop_times_df = gtfs_data['stop_times']
    calendar_df = gtfs_data['calendar']

    print(f"Loaded {len(stops_df)} stops, {len(routes_df)} routes")

    # Step 2: Create our nested structure
    ferry_data = {"stops": {}}

    # TODO: Fill in - loop through each stop and build the nested structure
    for _, stop in stops_df.iterrows():
        stop_id = str(stop['stop_id'])

        ferry_data["stops"][stop_id] = {
            "name": stop['stop_name'], 
            "lat": stop['stop_lat'], 
            "lon": stop['stop_lon'],
            "routes": {}
        }

      # Step 3: Add routes and schedules to each stop
    print("Processing routes and schedules...")

    # First, let's understand the service patterns
     
    service_patterns = {}
    for _, service in calendar_df.iterrows():
        service_id = service['service_id']
        # If it runs Mon-Fri, it's weekdays. If Sat-Sun, it's weekends
        if service['monday'] == 1:  # runs on weekdays
            service_patterns[service_id] = "weekdays"
        else:  # runs on weekends
            service_patterns[service_id] = "weekends"

    print(f"Service patterns: {service_patterns}")

    # Now for each stop, find what routes serve it
    for stop_id in ferry_data["stops"].keys():
         
        stop_visits = stop_times_df[stop_times_df['stop_id'] == int(stop_id)]
        
        # Group by route
        for _, visit in stop_visits.iterrows():
            trip_id = visit['trip_id']
            departure_time = visit['departure_time']

            # TODO: Fill in - find which route this trip belongs to
            trip_info = trips_df[trips_df['trip_id'] == trip_id].iloc[0]
            route_id = trip_info['route_id']
            service_id = trip_info['service_id']

            # Get route details
            route_info = routes_df[routes_df['route_id'] == route_id].iloc[0]

            # Add route to stop if not already there
            if route_id not in ferry_data["stops"][stop_id]["routes"]:
                ferry_data["stops"][stop_id]["routes"][route_id] = {
                    "route_name": route_info['route_long_name'],
                    "destinations": [],  # We'll fill this later
                    "schedule_patterns": {"weekdays": [], "weekends": []}
                }

            # Add departure time to appropriate pattern
            day_type = service_patterns[service_id]
            ferry_data["stops"][stop_id]["routes"][route_id]["schedule_patterns"][day_type].append(departure_time)

            

     

  # Calculate destinations once per route
    for stop_id in ferry_data["stops"].keys():
        for route_id in ferry_data["stops"][stop_id]["routes"].keys():

            # Find all trips for this route
            route_trips = trips_df[trips_df['route_id'] == route_id]
            destinations = set()

            for _, route_trip in route_trips.iterrows():
                trip_id = route_trip['trip_id']
                # Get all stops for this trip, ordered by sequence
                trip_stops = stop_times_df[stop_times_df['trip_id'] == trip_id].sort_values('stop_sequence')

                # Find current stop's position in this trip
                current_stop_rows = trip_stops[trip_stops['stop_id'] == int(stop_id)]
                if len(current_stop_rows) > 0:
                    current_sequence = current_stop_rows.iloc[0]['stop_sequence']
                    # Get stops that come after this one
                    future_stops = trip_stops[trip_stops['stop_sequence'] > current_sequence]
                    for _, future_stop in future_stops.iterrows():
                        destinations.add(str(future_stop['stop_id']))

            # Update destinations in our data structure
            ferry_data["stops"][stop_id]["routes"][route_id]["destinations"] = list(destinations)
        # Save to JSON file

    output_path = Path("data/ferry_data.json")
    output_path.parent.mkdir(exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(ferry_data, f, indent=2)

    print(f"Ferry data saved to {output_path}")

if __name__ == "__main__":
    asyncio.run(download_and_process_ferry_data())
# import asyncio 
# import httpx
# import zipfile
# import io
# import pandas as pd

# async def expole_ferry_gtfs(): 
#     #quick script to explore whats in the data
#     # 
#     # 
#     gtfs_routes_trips = "http://nycferry.connexionz.net/rtt/public/utility/gtfs.aspx"
#     async with httpx.AsyncClient() as client: 
#         print ("downloading ferry GTFS data...")
#         response = await client.get(gtfs_routes_trips, timeout=30.0, follow_redirects=True)
#         response.raise_for_status()

#         #look at whats in the zip 
#         zip_data = io.BytesIO(response.content)
#         with zipfile.ZipFile(zip_data, 'r') as zip_ref: 
#             print("\nFiles in GTFS Zip:")
#             for filename in zip_ref.namelist(): 
#                 print(f" - {filename}")
#             #look at each txt file structure
#             # 
#             for filename in zip_ref.namelist(): 
#                 if filename.endswith('.txt'): 
#                     print(f"\n==={filename}====")
#                     try: 
#                         file_data = zip_ref.read(filename)
#                         df = pd.read_csv(io.StringIO(file_data.decode('utf-8')))
#                         print (f"Columns: {list(df.columns)}")
#                         print(f"rows: {len(df)}")
#                         if len(df) > 0: 
#                             print("sample row: ")
#                             print(df.iloc[0].to_dict())
#                     except Exception as e: 
#                         print(f"Error reading {filename}: {e}") 

# if __name__ == "__main__": 
#     asyncio.run(expole_ferry_gtfs())
