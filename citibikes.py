import asyncio
import math 
from typing import Any 
import httpx
from mcp.server.fastmcp import FastMCP

#initialize FastMCP server
mcp = FastMCP("citibikes")

USER_AGENT = "citibikes-app/1.0"
gbfs_lyft_base = f"https://gbfs.lyft.com/gbfs/2.3/bkn/en"
gbfs_citi_base = f"https://gbfs.citibikenyc.com/gbfs/2.3/"


async def make_gbfs_request(url: str ) -> dict[str, Any]: 
    "make a request to different gbfs servers"
    headers = { 
        "User-Agent": USER_AGENT, 
        "Accept": "application/geo+json"
    }

    try: 
        async with httpx.AsyncClient() as client: 
            response = await client.get(url, headers = headers, timeout=10.0)
            response.raise_for_status()
            return response.json()
    except httpx.TimeoutException:
        return {"error": "Request timed out"}
    except httpx.HTTPError as e:
        return {"error": f"HTTP error: {e.response.status_code}"}
    except Exception as e:
        return {"error": str(e)}

def is_error(data): 
    return isinstance(data, dict) and "error" in data 

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

async def get_gbfs_feeds(): 
    "get discovery document showing all available feeds"
    url = f"{gbfs_citi_base}/gbfs.json"

    data = await make_gbfs_request(url)

    if isinstance(data, dict) and "error" in data: 
        return data
    
    return data 


async def get_station_info():
    "get static station information (locations, names, etc.)"
    url = f"{gbfs_lyft_base}/station_information.json"

    data = await make_gbfs_request(url)

    if isinstance(data, dict) and "error" in data: 
        return data
    
    return data 



async def get_station_status():
    "get real-time status information (availability, etc.)"
    url = f"{gbfs_lyft_base}/station_status.json"

    data = await make_gbfs_request(url)

    if isinstance(data, dict) and "error" in data: 
        return data
    
    return data 

async def find_nearby_stations(lat, lon, radius_km=0.5):
    "find stations within a certain radius of a latitude/longitude point" 


    stations = await get_station_info()
    status = await get_station_status()

    if is_error(stations) or is_error(status): 
        return []
    
    status_lookup = {s['station_id']: s for s in status['data']['stations']}

    nearby = [ ]

    for station in stations['data']['stations']:
        
        station_lat = station.get("lat")
        station_lon = station.get("lon")

        distance = haversine(lat, lon, station_lat, station_lon)
        if distance <= radius_km:
            station_status = status_lookup.get(station['station_id'], {})
            nearby.append({
                'name': station['name'],
                'lat': station_lat,
                'lon': station_lon,
                'distance_km': distance,
                'available_bikes': station_status.get("num_bikes_available", 0),
                'available_ebikes': station_status.get("num_ebikes_available", 0), 
                'available_docks': station_status.get("num_docks_available", 0),
                'station_id': station['station_id']
            })

    nearby.sort(key=lambda x: x['distance_km'])
    return nearby
    
@mcp.tool()
async def find_bikes_nearby(latitude: float, longitude: float, radius_km: float=0.5): 
    """
    Find available citi bike stations near a location 
    args: 
        latitude: latitude of location
        longitude: longitude of location
        radius_km: search radius in kilometers, default is 0.5km 
    """
    stations = await find_nearby_stations(latitude, longitude, radius_km)

    if not stations: 
        return "No citi bike stations found nearby or data unavailable. "
    
    stations_with_bikes = [s for s in stations if s['available_bikes'] > 0]

    if not stations_with_bikes: 
        return f"Found {len(stations)} stations nearby, but no bikes available right now. "

    #format for the agent
    result = f"Found {len(stations_with_bikes)} stations with bikes available: \n\n"

    for station in stations_with_bikes[:3]: 
        classic_bikes = station['available_bikes'] - station['available_ebikes']
        result += f"• {station['name']}\n"
        result += f"  Distance: {station['distance_km']}km\n"
        result += f"  Classic bikes: {classic_bikes} | E-Bikes available: {station['available_ebikes']}\n"
        result += f"  Docks available: {station['available_docks']}\n\n"

    return result 

@mcp.tool()
async def get_citibike_route_options(origin_lat: float, origin_lon: float, dest_lat: float, dest_lon: float, radius_km: float = 0.5) -> str:
    """Find Citi Bike pickup and dropoff options for a route.

    Args:
        origin_lat: Starting latitude
        origin_lon: Starting longitude
        dest_lat: Destination latitude  
        dest_lon: Destination longitude
        radius_km: Search radius in kilometers (default 0.5km)
    """

    pickup_stations = await find_nearby_stations(origin_lat, origin_lon, radius_km)
    dropoff_stations = await find_nearby_stations(dest_lat, dest_lon, radius_km)
    
    if not pickup_stations or not dropoff_stations:
        return "No Citi Bike stations found for this route or data unavailable."
    
    pickup_with_bikes = [s for s in pickup_stations if s['available_bikes'] > 0]
    dropoff_with_docks = [s for s in dropoff_stations if s['available_docks'] > 0]
    
    if not pickup_with_bikes:
        return "No bikes available at pickup locations."
    
    if not dropoff_with_docks:
        return "No dock space available at destination."
    
    result = " **PICKUP OPTIONS:**\n\n"
    
    for station in pickup_with_bikes[:2]:
        classic_bikes = station['available_bikes'] - station['available_ebikes']
        result += f"• {station['name']} ({station['distance_km']}km away)\n"
        result += f"  Classic: {classic_bikes} | E-bikes: {station['available_ebikes']}\n\n"
    
    result += " **DROPOFF OPTIONS:**\n\n"
    
    for station in dropoff_with_docks[:2]:
        result += f"• {station['name']} ({station['distance_km']}km away)\n"
        result += f"  Docks available: {station['available_docks']}\n\n"
    
    return result

# async def main(): 
#     # Test with Union Square coordinates
#     stations = await find_nearby_stations(40.739694, -73.980941, radius_km=0.3)
#     print(f"Found {len(stations)} stations within 300m:")
#     for station in stations[:3]:  # Show top 3
#         print(f"- {station['name']}: {station['distance_km']}km away")
#         print(f"  Bikes: {station['available_bikes']}, E-Bikes: {station['available_ebikes']}, Docks: {station['available_docks']}")

if __name__ == "__main__":
    mcp.run(transport='stdio')
    
##
