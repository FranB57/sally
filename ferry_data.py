import math 
from typing import Any 
import httpx 
import io 
import pandas as pd 
import zipfile 
import json
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("nyc_ferry_data")

USER_AGENT = "ferry_data-app/1.0"
#nyc Ferry uses GTFS 
#routes trips and stops 
gtfs_routes_trips = "http://nycferry.connexionz.net/rtt/public/utility/gtfs.aspx"
#REAL TIME ALERTS
gtfs_alerts = "http://nycferry.connexionz.net/rtt/public/utility/gtfsrealtime.aspx/alert" 
# REAL TIME TRIP UPDATES
gtfs_real_time_updates= "http://nycferry.connexionz.net/rtt/public/utility/gtfsrealtime.aspx/tripupdate"

with open('data/ferry_data.json', 'r') as f: 
    ferry_data = json.load(f)

async def make_gtfs_requests(url: str) -> dict[str, Any]: 
    """ make a request to different gtfs servers"""
    headers = {
        "User-Agent": USER_AGENT, 
        "Accept": "application/json"
    }

    try: 
        async with httpx.AsyncClient() as client: 
            response = await client.get(url, headers=headers, timeout=10.0, follow_redirects=True)
            response.raise_for_status()

            #for gtfs static daga (zip file)
            if url== gtfs_routes_trips:
                return {"content": response.content, "content_type":"zip"}
            else: 
                return {"content": response.content, "content_type": "protobuf"}
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

async def get_ferry_alerts():
    """Get real-time ferry service alerts"""
    data = await make_gtfs_requests(gtfs_alerts)
    
    if is_error(data):
        return data
    
    # TODO: Parse protobuf real-time data
    return {"raw_data": data["content"]}

async def get_ferry_trip_updates():
    """Get real-time ferry trip updates"""
    data = await make_gtfs_requests(gtfs_real_time_updates)
    
    if is_error(data):
        return data
    
    # TODO: Parse protobuf real-time data  
    return {"raw_data": data["content"]} 

async def get_nearby_ferry_stops(lat, lon, radius_km = 3):
    nearby_stops = []
    for stop_id, stop_info, in ferry_data["stops"].items(): 
        distance = haversine(lat, lon, stop_info["lat"], stop_info["lon"])

        if distance <= radius_km: 
            nearby_stops.append({
                "stop_id": stop_id, 
                "name": stop_info["name"],
                "distance_km": round(distance, 2),
                "routes": list(stop_info["routes"].keys()) 
            })
    nearby_stops.sort(key=lambda x: x["distance_km"])
    return nearby_stops

@mcp.tool()
async def find_ferry_stops_nearby(latitude: float, longitude: float, radius_km: float = 1.0):
    """
    Find ferry stops near a location

    Args:
        latitude: latitude of location
        longitude: longitude of location
        radius_km: search radius in kilometers, default 1.0km
    """
    # Convert string inputs to floats (handles quoted strings from LLMs)
    latitude = float(str(latitude).strip('"'))
    longitude = float(str(longitude).strip('"'))
    radius_km = float(str(radius_km).strip('"'))

    # Use the existing function we already wrote
    return await get_nearby_ferry_stops(latitude, longitude, radius_km)

@mcp.tool()
async def get_ferry_departures(stop_name_or_id: str):
    """
    Get departure times for a specific ferry stop

    Args:
        stop_name_or_id: Either the stop ID (like "87") or stop name (like "Wall St/Pier 11")
    """
    # First, find the stop - either by ID or by name
    target_stop_id = None
    target_stop_info = None

    # Check if it's a direct stop ID match
    if stop_name_or_id in ferry_data["stops"]:
        target_stop_id = stop_name_or_id
        target_stop_info = ferry_data["stops"][stop_name_or_id]
    else:
        # Search by name (case insensitive)
        for stop_id, stop_info in ferry_data["stops"].items():
            if stop_name_or_id.lower() in stop_info["name"].lower():
                target_stop_id = stop_id
                target_stop_info = stop_info
                break

    # If we didn't find the stop, return an error
    if not target_stop_id:
        return f"Stop '{stop_name_or_id}' not found. Try a stop ID or partial name."

    # Build the departure information
    result = {
        "stop_name": target_stop_info["name"],
        "stop_id": target_stop_id,
        "routes": []
    }

    # For each route serving this stop, get the schedule patterns
    for route_id, route_info in target_stop_info["routes"].items():
        route_result = {
            "route_id": route_id,
            "route_name": route_info["route_name"],
            "destinations": [],
            "schedules": {
                "weekdays": route_info["schedule_patterns"]["weekdays"][:10],  # Show first 10 times
                "weekends": route_info["schedule_patterns"]["weekends"][:10]
            }
        }

        # Add destination stop names (not just IDs)
        for dest_stop_id in route_info["destinations"]:
            if dest_stop_id in ferry_data["stops"]:
                route_result["destinations"].append({
                    "stop_id": dest_stop_id,
                    "name": ferry_data["stops"][dest_stop_id]["name"]
                })

        result["routes"].append(route_result)

    return result

@mcp.tool()
async def get_ferry_route_options(origin_lat: float, origin_lon: float, dest_lat: float, dest_lon: float, radius_km: float = 1.0):
    """
    Find ferry route options between two locations

    Args:
        origin_lat: Starting latitude
        origin_lon: Starting longitude
        dest_lat: Destination latitude
        dest_lon: Destination longitude
        radius_km: Search radius around each location in km
    """
    # Convert string inputs to floats (handles quoted strings from LLMs)
    origin_lat = float(str(origin_lat).strip('"'))
    origin_lon = float(str(origin_lon).strip('"'))
    dest_lat = float(str(dest_lat).strip('"'))
    dest_lon = float(str(dest_lon).strip('"'))
    radius_km = float(str(radius_km).strip('"'))

    # Find ferry stops near both origin and destination
    origin_stops = await get_nearby_ferry_stops(origin_lat, origin_lon, radius_km)
    dest_stops = await get_nearby_ferry_stops(dest_lat, dest_lon, radius_km)

    # If no stops found near either location, return helpful message
    if not origin_stops:
        return f"No ferry stops found within {radius_km}km of origin location."
    if not dest_stops:
        return f"No ferry stops found within {radius_km}km of destination location."

    # Find route options
    route_options = []

    # Check for direct routes (same route serves both origin and destination stops)
    for origin_stop in origin_stops:
        for dest_stop in dest_stops:
            # Get routes that serve the origin stop
            origin_routes = set(origin_stop["routes"])
            # Get routes that serve the destination stop
            dest_routes = set(dest_stop["routes"])

            # Find shared routes (direct connection possible)
            shared_routes = origin_routes & dest_routes

            for route_id in shared_routes:
                # Check if destination stop is actually reachable from origin stop on this route
                origin_stop_info = ferry_data["stops"][origin_stop["stop_id"]]
                route_info = origin_stop_info["routes"][route_id]

                # If destination stop is in the destinations list, it's reachable
                if dest_stop["stop_id"] in route_info["destinations"]:
                    route_options.append({
                        "type": "direct",
                        "route_id": route_id,
                        "route_name": route_info["route_name"],
                        "origin_stop": {
                            "name": origin_stop["name"],
                            "distance_km": origin_stop["distance_km"]
                        },
                        "dest_stop": {
                            "name": dest_stop["name"],
                            "distance_km": dest_stop["distance_km"]
                        },
                        "total_walk_distance_km": round(origin_stop["distance_km"] + dest_stop["distance_km"], 2)
                    })

    # Sort by total walking distance (closest stops first)
    route_options.sort(key=lambda x: x["total_walk_distance_km"])

    # If no direct routes found, suggest closest stops
    if not route_options:
        return {
            "message": "No direct ferry routes found between these locations.",
            "suggestions": {
                "closest_origin_stops": origin_stops[:3],  # Show 3 closest
                "closest_dest_stops": dest_stops[:3]
            }
        }

    return {
        "route_options": route_options[:5],  # Show up to 5 best options
        "total_options_found": len(route_options)
    }

if __name__ == "__main__":
    mcp.run(transport='stdio')