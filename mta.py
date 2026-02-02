import math 
from typing import Any 
import httpx
from mcp.server.fastmcp import FastMCP
from nyct_gtfs import NYCTFeed
import pandas as pd 
from datetime import datetime

FEEDS_CACHE = {} 
def get_cached_feed(feed_key): 
    if feed_key not in FEEDS_CACHE: 
        # print(f"Creating NEW feed for {feed_key}")
        FEEDS_CACHE[feed_key] = NYCTFeed(feed_key)
    # else: 
        # print(f"Using CACHED feed for {feed_key}")
    return FEEDS_CACHE[feed_key]

STATIONS_BY_ID = None
def initialize_station_cache(): 
    global STATIONS_BY_ID
    if STATIONS_BY_ID is None: 
        STATIONS_BY_ID = stations_df.set_index('Complex ID').to_dict('index')

#initialize FastMCP server
mcp = FastMCP("mta_subway")
data_path = "data/MTA_Subway_Stations_and_Complexes_20250916.csv"
stations_df = pd.read_csv(data_path)

initialize_station_cache()

subway_lines_dict = { 
    "A": ["A","C", "E"],
    "G": ["G"],
    "J": ["J","Z"], 
    "N": ["N", 'Q', 'R', 'W'], 
    "1": ['1','2','3','4','5','6','7','S'],
    "L": ['L'],
    "B": ['B','D','F','M'],
    "SIR" :['SIR']
}

def build_transfer_stations():
    """Build transfer stations dict from STATIONS_BY_ID (stations with 2+ routes)."""
    transfers = {}
    for complex_id, info in STATIONS_BY_ID.items():
        routes = info['Daytime Routes'].split()
        if len(routes) >= 2:
            transfers[complex_id] = {'lines': routes, 'name': info['Stop Name']}
    return transfers

TRANSFER_STATIONS = build_transfer_stations()

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    Returns distance in kilometers
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    # Radius of earth in kilometers
    r = 6371
    
    return c * r

def get_train_times_by_complex_id(complex_ID): 
    target_station = stations_df.loc[stations_df['Complex ID'] == complex_ID]

    # Convert the 'col_obj' column to the dedicated Pandas StringDtype. and split the string 
    gtfs_stopID_raw = target_station["GTFS Stop IDs"].iloc[0]
    base_stopID = gtfs_stopID_raw.split('; ')
    lines_raw = target_station["Daytime Routes"].iloc[0]
    lines = lines_raw.split(' ')

    #add direction suffixes
    #trains api accepts stopID+N for northbound or S for southbound 
    target_stopID = []
    for stop_id in base_stopID:
        target_stopID.extend([f"{stop_id}N", f"{stop_id}S"])
    

    feeds_needed = set()
    # Loop through lines
    for line in lines:
        # Find which feed this line belongs to
        for feed_key, feed_lines in subway_lines_dict.items():
            if line in feed_lines:
                feeds_needed.add(feed_key)  # set automatically handles duplicates
                break

    all_arrivals = [] # collect all arrivals 
    
    # loop through set and initialize feeds, print results
    for feed_key in feeds_needed: 
        
        feed = get_cached_feed(feed_key)
        trains = feed.filter_trips(headed_for_stop_id=target_stopID)

        # print(f"Found {len(trains)} trains")

        # extract arrival times: 
        
        for trip in trains: 
            for stop_update in trip.stop_time_updates: 
                if stop_update.stop_id in target_stopID:
                    direction = "Uptown" if stop_update.stop_id.endswith("N") else "Downtown"
                    all_arrivals.append({
                        'route': trip.route_id,
                        'direction': direction,
                        'stop_name': stop_update.stop_name,
                        'arrival_time': stop_update.arrival,
                        'departure_time': stop_update.departure,
                        'delay': getattr(stop_update, 'delay', 0)
                    })
    
    all_arrivals.sort(key=lambda x: x['arrival_time'] if x['arrival_time'] else datetime.max)

    return all_arrivals[:15]


def find_nearest_stations(latitude: float, longitude: float, radius_km: float=0.5):
    """
    Find nearby subway stations. 
    """
    #pre filter by rough bounding box first
    lat_margin = radius_km / 111.0 
    lon_margin = radius_km / (111.0 * math.cos(math.radians(latitude)))

    bounded_df = stations_df[
        (stations_df['Latitude'] >= latitude - lat_margin) &
        (stations_df['Latitude'] <= latitude + lat_margin) &
        (stations_df['Longitude'] >= longitude - lon_margin) &
        (stations_df['Longitude'] <= longitude + lon_margin)
    ][['Complex ID', 'Latitude', 'Longitude']].copy()
    
    # Now apply haversine only to the subset
    bounded_df['distance_to_user'] = bounded_df.apply(
        lambda row: haversine(latitude, longitude, row['Latitude'], row['Longitude']), axis=1
    )

    nearby = bounded_df[bounded_df['distance_to_user']<= radius_km]
    return nearby[['Complex ID', 'distance_to_user']].to_dict('records')
     

@mcp.tool()
async def get_nearby_subway_options(lat, lon, radius_km=0.5, max_stations=10): 
    """ 
    Find nearby and list of incoming trains for mta subway stations for a location

    Args: 
        lat: starting latitude
        lon:" starting longitude 
        radius_km = search radius in km, default is 0,5km 
        max_stations: limit number of stations to return  
    
    """
    lat = float(str(lat).strip('"'))
    lon = float(str(lon).strip('"'))
    radius_km = float(str(radius_km).strip('"'))
    max_stations = int(str(max_stations).strip('"'))
    

    #get all nearby stations 
    nearby_stations = find_nearest_stations(lat, lon, radius_km)
    nearby_stations = sorted(nearby_stations, key=lambda x: x['distance_to_user'])[:max_stations]
    nearby_stations_with_arrivals = []

    for station in nearby_stations: 
        #get station metadata 
        # station_info = stations_df.loc[stations_df['Complex ID']== station['Complex ID']].iloc[0]
        station_info = STATIONS_BY_ID[station['Complex ID']]

        arrivals = get_train_times_by_complex_id(station['Complex ID'])
        formatted_arrivals = []
        for arrival in arrivals: 
            if arrival['arrival_time']: 
                formatted_arrivals.append({
                    'route': arrival['route'], 
                    'direction': arrival['direction'],
                    'arrival_time': arrival['arrival_time'].strftime('%I:%M %p'),
                    'minutes_away': int((arrival['arrival_time'] - datetime.now()).total_seconds() /60 )
                })

        nearby_stations_with_arrivals.append({
            'complex_id': station['Complex ID'], 
            'station_name': station_info['Stop Name'], 
            'distance_km': round(station['distance_to_user'], 2 ), 
            'walk_time_mins' : int(station['distance_to_user'] * 12), #~12 min per km  
            'next_trains': formatted_arrivals[:10] #limit trains show
        })
    return nearby_stations_with_arrivals

@mcp.tool()
async def get_subway_route_options(origin_lat: float, origin_lon: float, 
                                 dest_lat: float, dest_lon: float, 
                                 max_options: int = 5):
    """
    Find subway routing options between two locations with transfer details
    
    Args:
        origin_lat: Starting latitude
        origin_lon: Starting longitude  
        dest_lat: Destination latitude
        dest_lon: Destination longitude
        max_options: Maximum number of route options to return
    """
    
    # Get nearby stations for both origin and destination
    origin_stations = find_nearest_stations(origin_lat, origin_lon, 0.8)[:4]
    dest_stations = find_nearest_stations(dest_lat, dest_lon, 0.8)[:4]
    
    if not origin_stations or not dest_stations:
        return "No nearby subway stations found for this route."
    
    route_options = []
    
    # Check direct routes first (same line connects origin and destination)
    for orig in origin_stations:
        orig_data = STATIONS_BY_ID[orig['Complex ID']]
        orig_lines = set(orig_data['Daytime Routes'].split())
        
        for dest in dest_stations:
            dest_data = STATIONS_BY_ID[dest['Complex ID']]
            dest_lines = set(dest_data['Daytime Routes'].split())
            
            # Find shared lines (direct connection)
            shared_lines = orig_lines & dest_lines
            if shared_lines:
                route_options.append({
                    'type': 'direct',
                    'description': f"Take {'/'.join(sorted(shared_lines))} from {orig_data['Stop Name']} directly to {dest_data['Stop Name']}",
                    'origin_station': orig_data['Stop Name'],
                    'dest_station': dest_data['Stop Name'],
                    'lines': sorted(list(shared_lines)),
                    'walk_to_origin_mins': round(orig['distance_to_user'] * 12),
                    'walk_from_dest_mins': round(dest['distance_to_user'] * 12),
                    'total_walk_mins': round((orig['distance_to_user'] + dest['distance_to_user']) * 12)
                })
    
    # Single transfer routes via major hubs
    for orig in origin_stations:
        orig_data = STATIONS_BY_ID[orig['Complex ID']]
        orig_lines = set(orig_data['Daytime Routes'].split())
        
        for dest in dest_stations:
            dest_data = STATIONS_BY_ID[dest['Complex ID']]
            dest_lines = set(dest_data['Daytime Routes'].split())
            
            # Skip if we already found a direct route between these stations
            if orig_lines & dest_lines:
                continue
                
            # Check each transfer hub
            for hub_id, hub_info in TRANSFER_STATIONS.items():
                hub_lines = set(hub_info['lines'])
                
                # Can we get from origin to hub?
                orig_to_hub = orig_lines & hub_lines
                # Can we get from hub to destination?
                hub_to_dest = hub_lines & dest_lines
                
                if orig_to_hub and hub_to_dest:
                    route_options.append({
                        'type': 'transfer',
                        'description': f"Take {'/'.join(sorted(orig_to_hub))} from {orig_data['Stop Name']} to {hub_info['name']}, transfer to {'/'.join(sorted(hub_to_dest))} to {dest_data['Stop Name']}",
                        'origin_station': orig_data['Stop Name'],
                        'transfer_station': hub_info['name'],
                        'dest_station': dest_data['Stop Name'],
                        'first_leg_lines': sorted(list(orig_to_hub)),
                        'second_leg_lines': sorted(list(hub_to_dest)),
                        'walk_to_origin_mins': round(orig['distance_to_user'] * 12),
                        'walk_from_dest_mins': round(dest['distance_to_user'] * 12),
                        'total_walk_mins': round((orig['distance_to_user'] + dest['distance_to_user']) * 12)
                    })
    
    # Remove duplicates based on description
    unique_routes = []
    seen_descriptions = set()
    
    for route in route_options:
        if route['description'] not in seen_descriptions:
            seen_descriptions.add(route['description'])
            unique_routes.append(route)
    
    # Sort routes: direct first, then by total walking time
    unique_routes.sort(key=lambda x: (
        x['type'] != 'direct',  # Direct routes first
        x['total_walk_mins']    # Then by walking time
    ))
    
    return unique_routes[:max_options]

if __name__ == "__main__" :
    mcp.run(transport='stdio')


# print(get_nearby_subway_options(40.7417,-73.9847))