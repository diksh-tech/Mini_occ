# server.py
import os
import logging
import json
from typing import Optional, Any, Dict
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
load_dotenv() 

from mcp.server.fastmcp import FastMCP

HOST = os.getenv("MCP_HOST", "127.0.0.1")
PORT = int(os.getenv("MCP_PORT", "8000"))
TRANSPORT = os.getenv("MCP_TRANSPORT", "streamable-http")

MONGODB_URL = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("MONGO_DB")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("flightops.mcp.server")

mcp = FastMCP("FlightOps MCP Server")

_mongo_client: Optional[AsyncIOMotorClient] = None
_db = None
_col = None

async def get_mongodb_client():
    """Initialize and return the global Motor client, DB and collection."""
    global _mongo_client, _db, _col
    if _mongo_client is None:
        logger.info("Connecting to MongoDB: %s", MONGODB_URL)
        _mongo_client = AsyncIOMotorClient(MONGODB_URL)
        _db = _mongo_client[DATABASE_NAME]
        _col = _db[COLLECTION_NAME]
    return _mongo_client, _db, _col

def normalize_flight_number(flight_number: Any) -> Optional[int]:
    """Convert flight_number to int. MongoDB stores it as int."""
    if flight_number is None or flight_number == "":
        return None
    if isinstance(flight_number, int):
        return flight_number
    try:
        return int(str(flight_number).strip())
    except (ValueError, TypeError):
        logger.warning(f"Could not normalize flight_number: {flight_number}")
        return None

def validate_date(date_str: str) -> Optional[str]:
    """
    Validate date_of_origin string. Accepts common formats.
    Returns normalized ISO date string YYYY-MM-DD if valid, else None.
    """
    if not date_str or date_str == "":
        return None
    
    # Handle common date formats
    formats = [
        "%Y-%m-%d",      # 2024-06-23
        "%d-%m-%Y",      # 23-06-2024
        "%Y/%m/%d",      # 2024/06/23
        "%d/%m/%Y",      # 23/06/2024
        "%B %d, %Y",     # June 23, 2024
        "%d %B %Y",      # 23 June 2024
        "%b %d, %Y",     # Jun 23, 2024
        "%d %b %Y"       # 23 Jun 2024
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    
    logger.warning(f"Could not parse date: {date_str}")
    return None

def make_query(carrier: str, flight_number: Optional[int], date_of_origin: str) -> Dict:
    """
    Build MongoDB query matching the actual database schema.
    """
    query = {}
    
    # Add carrier if provided
    if carrier:
        query["flightLegState.carrier"] = carrier
    
    # Add flight number as integer (as stored in DB)
    if flight_number is not None:
        query["flightLegState.flightNumber"] = flight_number
    
    # Add date if provided
    if date_of_origin:
        query["flightLegState.dateOfOrigin"] = date_of_origin
    
    logger.info(f"Built query: {json.dumps(query)}")
    return query

def response_ok(data: Any) -> str:
    """Return JSON string for successful response."""
    return json.dumps({"ok": True, "data": data}, indent=2, default=str)

def response_error(msg: str, code: int = 400) -> str:
    """Return JSON string for error response."""
    return json.dumps({"ok": False, "error": {"message": msg, "code": code}}, indent=2)

async def _fetch_one_async(query: dict, projection: dict) -> str:
    """
    Consistent async DB fetch and error handling.
    Returns JSON string response.
    """
    try:
        _, _, col = await get_mongodb_client()
        logger.info(f"Executing query: {json.dumps(query)}")
        
        result = await col.find_one(query, projection)
        
        if not result:
            logger.warning(f"No document found for query: {json.dumps(query)}")
            return response_error("No matching document found.", code=404)
        
        # Remove _id and _class to keep output clean
        if "_id" in result:
            result.pop("_id")
        if "_class" in result:
            result.pop("_class")
        
        logger.info(f"Query successful")
        return response_ok(result)
    except Exception as exc:
        logger.exception("DB query failed")
        return response_error(f"DB query failed: {str(exc)}", code=500)

def format_delay_summary_professional(flight_data: dict, selected_route: str) -> str:
    """
    Format delay summary in a professional, user-friendly way without markdown.
    """
    try:
        # Extract flight information
        carrier = flight_data.get("carrier", "Unknown")
        flight_number = flight_data.get("flightNumber", "Unknown")
        date_of_origin = flight_data.get("dateOfOrigin", "Unknown")
        start_station = flight_data.get("startStation", "Unknown")
        end_station = flight_data.get("endStation", "Unknown")
        flight_status = flight_data.get("flightStatus", "Unknown")
        scheduled_time = flight_data.get("scheduledStartTime", "Unknown")
        
        # Format scheduled time if it's in ISO format
        if scheduled_time != "Unknown":
            try:
                if "T" in scheduled_time:
                    dt = datetime.fromisoformat(scheduled_time.replace('Z', '+00:00'))
                    scheduled_time = dt.strftime("%I:%M %p, %b %d, %Y")
            except:
                pass
        
        # Extract delays information
        delays = flight_data.get("delays", {})
        delay_list = []
        total_delay = "0 minutes"
        
        # Parse delays based on different possible structures
        if isinstance(delays, dict):
            delay_list = delays.get("delay", [])
            total_delay = delays.get("total", "PT0M")
        elif isinstance(delays, list):
            delay_list = delays
        
        # Convert ISO duration to readable format
        def parse_duration(duration_str):
            """Convert PT53M to 53 minutes, PT1H30M to 1 hour 30 minutes, etc."""
            if not duration_str or not isinstance(duration_str, str):
                return "0 minutes"
            
            try:
                # Remove 'PT' prefix
                if duration_str.startswith('PT'):
                    duration_str = duration_str[2:]
                
                hours = 0
                minutes = 0
                
                # Check for hours
                if 'H' in duration_str:
                    hours_part = duration_str.split('H')[0]
                    hours = int(hours_part) if hours_part.isdigit() else 0
                    duration_str = duration_str.split('H')[-1]
                
                # Check for minutes
                if 'M' in duration_str:
                    minutes_part = duration_str.split('M')[0]
                    minutes = int(minutes_part) if minutes_part.isdigit() else 0
                
                if hours > 0 and minutes > 0:
                    return f"{hours} hour{'s' if hours > 1 else ''} {minutes} minute{'s' if minutes > 1 else ''}"
                elif hours > 0:
                    return f"{hours} hour{'s' if hours > 1 else ''}"
                elif minutes > 0:
                    return f"{minutes} minute{'s' if minutes > 1 else ''}"
                else:
                    return "0 minutes"
                    
            except Exception:
                return duration_str  # Return original if parsing fails
        
        # Format total delay
        total_delay_readable = parse_duration(total_delay)
        
        # Build professional summary without markdown
        summary_parts = []
        
        # Header
        summary_parts.append("🛬 FLIGHT DELAY ANALYSIS REPORT")
        summary_parts.append("")
        
        # Flight Information
        summary_parts.append("FLIGHT DETAILS")
        summary_parts.append(f"Flight: {carrier}{flight_number}")
        summary_parts.append(f"Date: {date_of_origin}")
        summary_parts.append(f"Route: {start_station} → {end_station}")
        summary_parts.append(f"Scheduled Time: {scheduled_time}")
        summary_parts.append(f"Status: {flight_status}")
        summary_parts.append("")
        
        # Delay Summary
        summary_parts.append("DELAY SUMMARY")
        summary_parts.append(f"Total Delay Duration: {total_delay_readable}")
        summary_parts.append("")
        
        # Individual Delays
        if delay_list:
            summary_parts.append("DETAILED DELAY BREAKDOWN")
            
            for i, delay in enumerate(delay_list, 1):
                if isinstance(delay, dict):
                    reason = delay.get('reason', 'Unknown Reason')
                    duration = parse_duration(delay.get('time', 'PT0M'))
                    remark = delay.get('remark', 'No additional remarks')
                    delay_number = delay.get('delayNumber', i)
                    is_root_cause = delay.get('isRootCause', False)
                    
                    summary_parts.append(f"{i}. Delay #{delay_number}")
                    summary_parts.append(f"   Reason: {reason}")
                    summary_parts.append(f"   Duration: {duration}")
                    summary_parts.append(f"   Remarks: {remark}")
                    if is_root_cause:
                        summary_parts.append(f"   Impact: Root Cause Delay")
                    summary_parts.append("")
        else:
            summary_parts.append("No individual delay records found.")
            summary_parts.append("")
        
        # Operational Impact
        summary_parts.append("OPERATIONAL IMPACT")
        if total_delay_readable != "0 minutes":
            summary_parts.append(f"• This flight experienced a total delay of {total_delay_readable}")
            if delay_list:
                summary_parts.append(f"• {len(delay_list)} delay event{'s' if len(delay_list) > 1 else ''} were recorded")
            summary_parts.append("• Delays may impact connecting flights and ground operations")
        else:
            summary_parts.append("• No significant delays recorded for this flight")
            summary_parts.append("• Operations were as per schedule")
        
        summary_parts.append("")
        summary_parts.append("---")
        summary_parts.append("Report generated by FlightOps Analytics")
        
        return "\n".join(summary_parts)
        
    except Exception as e:
        logger.error(f"Error in professional formatting: {e}")
        # Fallback to basic formatting
        basic_summary = f"Delay Summary for {selected_route}\n"
        basic_summary += f"Flight: {flight_data.get('carrier', '')}{flight_data.get('flightNumber', '')}\n"
        basic_summary += f"Total Delay: {flight_data.get('delays', {}).get('total', 'PT0M')}"
        return basic_summary

# --- MCP Tools ---

@mcp.tool()
async def health_check() -> str:
    """
    Simple health check for orchestrators and clients.
    Attempts a cheap DB ping.
    """
    try:
        _, _, col = await get_mongodb_client()
        doc = await col.find_one({}, {"_id": 1})
        return response_ok({"status": "ok", "db_connected": doc is not None})
    except Exception as e:
        logger.exception("Health check DB ping failed")
        return response_error("DB unreachable", code=503)

@mcp.tool()
async def get_flight_basic_info(carrier: str = "", flight_number: str = "", date_of_origin: str = "") -> str:
    """
    Fetch basic flight information including carrier, flight number, date, stations, times, and status.
    
    Args:
        carrier: Airline carrier code (e.g., "6E", "AI")
        flight_number: Flight number as string (e.g., "215")
        date_of_origin: Date in YYYY-MM-DD format (e.g., "2024-06-23")
    """
    logger.info(f"get_flight_basic_info: carrier={carrier}, flight_number={flight_number}, date={date_of_origin}")
    
    # Normalize inputs
    fn = normalize_flight_number(flight_number) if flight_number else None
    dob = validate_date(date_of_origin) if date_of_origin else None
    
    if date_of_origin and not dob:
        return response_error("Invalid date_of_origin format. Expected YYYY-MM-DD or common date formats", 400)
    
    query = make_query(carrier, fn, dob)
    
    # Project basic flight information
    projection = {
        "flightLegState.carrier": 1,
        "flightLegState.flightNumber": 1,
        "flightLegState.suffix": 1,
        "flightLegState.dateOfOrigin": 1,
        "flightLegState.seqNumber": 1,
        "flightLegState.startStation": 1,
        "flightLegState.endStation": 1,
        "flightLegState.startStationICAO": 1,
        "flightLegState.endStationICAO": 1,
        "flightLegState.scheduledStartTime": 1,
        "flightLegState.scheduledEndTime": 1,
        "flightLegState.flightStatus": 1,
        "flightLegState.operationalStatus": 1,
        "flightLegState.flightType": 1,
        "flightLegState.blockTimeSch": 1,
        "flightLegState.blockTimeActual": 1,
        "flightLegState.flightHoursActual": 1,
        "flightLegState.isOTPFlight": 1,
        "flightLegState.isOTPAchieved": 1,
        "flightLegState.isOTPConsidered": 1,
        "flightLegState.isOTTFlight": 1,
        "flightLegState.isOTTAchievedFlight": 1,
        "flightLegState.turnTimeFlightBeforeActual": 1,
        "flightLegState.turnTimeFlightBeforeSch": 1
    }
    
    return await _fetch_one_async(query, projection)

@mcp.tool()
async def get_operation_times(carrier: str = "", flight_number: str = "", date_of_origin: str = "") -> str:
    """
    Return estimated and actual operation times for a flight including takeoff, landing, block times,StartTimeOffset, EndTimeOffset.
    
    Args:
        carrier: Airline carrier code
        flight_number: Flight number as string
        date_of_origin: Date in YYYY-MM-DD format
    """
    logger.info(f"get_operation_times: carrier={carrier}, flight_number={flight_number}, date={date_of_origin}")
    
    fn = normalize_flight_number(flight_number) if flight_number else None
    dob = validate_date(date_of_origin) if date_of_origin else None
    
    if date_of_origin and not dob:
        return response_error("Invalid date format.", 400)
    
    query = make_query(carrier, fn, dob)
    
    projection = {
        "flightLegState.carrier": 1,
        "flightLegState.flightNumber": 1,
        "flightLegState.dateOfOrigin": 1,
        "flightLegState.startStation": 1,
        "flightLegState.endStation": 1,
        "flightLegState.scheduledStartTime": 1,
        "flightLegState.scheduledEndTime": 1,
        "flightLegState.startTimeOffset": 1,
        "flightLegState.endTimeOffset": 1,
        "flightLegState.operation.estimatedTimes": 1,
        "flightLegState.operation.actualTimes": 1,
        "flightLegState.taxiOutTime": 1,
        "flightLegState.taxiInTime": 1,
        "flightLegState.blockTimeSch": 1,
        "flightLegState.blockTimeActual": 1,
        "flightLegState.flightHoursActual": 1,
    }
    
    return await _fetch_one_async(query, projection)

@mcp.tool()
async def get_equipment_info(carrier: str = "", flight_number: str = "", date_of_origin: str = "") -> str:
    """
    Get aircraft equipment details including aircraft type, registration (tail number), and configuration.
    
    Args:
        carrier: Airline carrier code
        flight_number: Flight number as string
        date_of_origin: Date in YYYY-MM-DD format
    """
    logger.info(f"get_equipment_info: carrier={carrier}, flight_number={flight_number}, date={date_of_origin}")
    
    fn = normalize_flight_number(flight_number) if flight_number else None
    dob = validate_date(date_of_origin) if date_of_origin else None
    
    query = make_query(carrier, fn, dob)
    
    projection = {
        "flightLegState.carrier": 1,
        "flightLegState.flightNumber": 1,
        "flightLegState.dateOfOrigin": 1,
        "flightLegState.equipment.plannedAircraftType": 1,
        "flightLegState.equipment.aircraft": 1,
        "flightLegState.equipment.aircraftConfiguration": 1,
        "flightLegState.equipment.aircraftRegistration": 1,
        "flightLegState.equipment.assignedAircraftTypeIATA": 1,
        "flightLegState.equipment.assignedAircraftTypeICAO": 1,
        "flightLegState.equipment.assignedAircraftTypeIndigo": 1,
        "flightLegState.equipment.assignedAircraftConfiguration": 1,
        "flightLegState.equipment.tailLock": 1,
        "flightLegState.equipment.onwardFlight": 1,
        "flightLegState.equipment.actualOnwardFlight": 1
    }
    
    return await _fetch_one_async(query, projection)

@mcp.tool()
async def get_delay_summary(carrier: str = "", flight_number: str = "", date_of_origin: str = "", selected_route: str = "") -> str:
    """
    Summarize delay reasons, durations, and total delay time for a specific flight.
    If multiple routes exist, returns route options for user selection.
    
    Args:
        carrier: Airline carrier code (e.g., "6E", "AI")
        flight_number: Flight number as string (e.g., "215")
        date_of_origin: Date in YYYY-MM-DD format (e.g., "2024-06-23")
        selected_route: Route in format "START_STATION-END_STATION" (optional, for when user selects from multiple options)
    """
    logger.info(f"get_delay_summary: carrier={carrier}, flight_number={flight_number}, date={date_of_origin}, selected_route={selected_route}")
    
    # Normalize inputs
    fn = normalize_flight_number(flight_number) if flight_number else None
    dob = validate_date(date_of_origin) if date_of_origin else None
    
    if date_of_origin and not dob:
        return response_error("Invalid date_of_origin format. Expected YYYY-MM-DD or common date formats", 400)
    
    # Step 1: Build base query
    base_query = make_query(carrier, fn, dob)
    
    try:
        _, _, col = await get_mongodb_client()
        
        # Step 2: Count documents for this flight
        count = await col.count_documents(base_query)
        logger.info(f"Found {count} documents for query: {json.dumps(base_query)}")
        
        # Step 3: If multiple documents and no route selected, return route options ONLY
        if count > 1 and not selected_route:
            # Get minimal route information for selection
            cursor = col.find(base_query, {
                "flightLegState.startStation": 1,
                "flightLegState.endStation": 1,
                "flightLegState.scheduledStartTime": 1,
                "flightLegState.flightStatus": 1,
                "_id": 0
            })
            
            routes = []
            async for doc in cursor:
                route_info = {
                    "route_id": f"{doc.get('flightLegState', {}).get('startStation', '')}-{doc.get('flightLegState', {}).get('endStation', '')}",
                    "startStation": doc.get("flightLegState", {}).get("startStation", "Unknown"),
                    "endStation": doc.get("flightLegState", {}).get("endStation", "Unknown"),
                    "scheduledStartTime": doc.get("flightLegState", {}).get("scheduledStartTime", "Unknown"),
                    "flightStatus": doc.get("flightLegState", {}).get("flightStatus", "Unknown")
                }
                # Only add unique routes
                if not any(r["route_id"] == route_info["route_id"] for r in routes):
                    routes.append(route_info)
            
            return response_ok({
                "status": "route_selection_required",
                "multiple_routes": True,
                "message": f"Found {len(routes)} different routes for flight {carrier}{flight_number} on {date_of_origin}.",
                "available_routes": routes,
                "instruction": "Please select a route by providing the route number (1, 2, 3, etc.)",
                "next_step": "Call this tool again with the selected_route parameter"
            })
        
        # Step 4: If single document or route selected, proceed with actual delay summary
        final_query = base_query.copy()
        if selected_route:
            try:
                start_station, end_station = selected_route.split('-')
                final_query["flightLegState.startStation"] = start_station
                final_query["flightLegState.endStation"] = end_station
                logger.info(f"Query modified with selected route: {start_station} -> {end_station}")
            except ValueError:
                return response_error("Invalid selected_route format. Expected 'START_STATION-END_STATION'", 400)
        
        # Step 5: Fetch the actual delay summary
        projection = {
            "flightLegState.carrier": 1,
            "flightLegState.flightNumber": 1,
            "flightLegState.dateOfOrigin": 1,
            "flightLegState.startStation": 1,
            "flightLegState.endStation": 1,
            "flightLegState.scheduledStartTime": 1,
            "flightLegState.operation.actualTimes.offBlock": 1,
            "flightLegState.delays": 1,
            "flightLegState.flightStatus": 1,
            "flightLegState.operationalStatus": 1,
            "flightLegState.startTimeOffset": 1,
            "flightLegState.endTimeOffset": 1
        }
        
        # Use the actual fetch function and get the raw result
        raw_result = await _fetch_one_async(final_query, projection)
        
        # Step 6: Format the response in a professional, user-friendly way
        if isinstance(raw_result, str):
            try:
                result_dict = json.loads(raw_result)
                if result_dict.get("ok") and result_dict.get("data"):
                    data = result_dict["data"]
                    flight_data = data.get("flightLegState", {})
                    
                    # Format the delay summary professionally
                    formatted_summary = format_delay_summary_professional(flight_data, selected_route)
                    
                    # Return ONLY the formatted summary, not nested structure
                    return response_ok({
                        "formatted_summary": formatted_summary,
                        "selected_route": selected_route if selected_route else "auto_selected"
                    })
                else:
                    # If there's an error in the original result, return it
                    return raw_result
            except Exception as e:
                logger.error(f"Error formatting delay summary: {e}")
                # If formatting fails, return original result
                return raw_result
        
        return raw_result
        
    except Exception as e:
        logger.exception("Error in get_delay_summary")
        return response_error(f"Database error: {str(e)}", 500)

@mcp.tool()
async def get_fuel_summary(carrier: str = "", flight_number: str = "", date_of_origin: str = "") -> str:
    """
    Retrieve fuel summary including planned vs actual fuel for takeoff, landing, and total consumption.
    
    Args:
        carrier: Airline carrier code
        flight_number: Flight number as string
        date_of_origin: Date in YYYY-MM-DD format
    """
    logger.info(f"get_fuel_summary: carrier={carrier}, flight_number={flight_number}, date={date_of_origin}")
    
    fn = normalize_flight_number(flight_number) if flight_number else None
    dob = validate_date(date_of_origin) if date_of_origin else None
    
    query = make_query(carrier, fn, dob)
    
    projection = {
        "flightLegState.carrier": 1,
        "flightLegState.flightNumber": 1,
        "flightLegState.dateOfOrigin": 1,
        "flightLegState.startStation": 1,
        "flightLegState.endStation": 1,
        "flightLegState.operation.fuel": 1,
        "flightLegState.operation.flightPlan.offBlockFuel": 1,
        "flightLegState.operation.flightPlan.takeoffFuel": 1,
        "flightLegState.operation.flightPlan.landingFuel": 1,
        "flightLegState.operation.flightPlan.holdFuel": 1
    }
    
    return await _fetch_one_async(query, projection)

@mcp.tool()
async def get_passenger_info(carrier: str = "", flight_number: str = "", date_of_origin: str = "") -> str:
    """
    Get passenger count and connection information for the flight.
    
    Args:
        carrier: Airline carrier code
        flight_number: Flight number as string
        date_of_origin: Date in YYYY-MM-DD format
    """
    logger.info(f"get_passenger_info: carrier={carrier}, flight_number={flight_number}, date={date_of_origin}")
    
    fn = normalize_flight_number(flight_number) if flight_number else None
    dob = validate_date(date_of_origin) if date_of_origin else None
    
    query = make_query(carrier, fn, dob)
    
    projection = {
        "flightLegState.carrier": 1,
        "flightLegState.flightNumber": 1,
        "flightLegState.dateOfOrigin": 1,
        "flightLegState.pax": 1
    }
    
    return await _fetch_one_async(query, projection)

@mcp.tool()
async def get_crew_info(carrier: str = "", flight_number: str = "", date_of_origin: str = "") -> str:
    """
    Get crew connections and details for the flight.
    
    Args:
        carrier: Airline carrier code
        flight_number: Flight number as string
        date_of_origin: Date in YYYY-MM-DD format
    """
    logger.info(f"get_crew_info: carrier={carrier}, flight_number={flight_number}, date={date_of_origin}")
    
    fn = normalize_flight_number(flight_number) if flight_number else None
    dob = validate_date(date_of_origin) if date_of_origin else None
    
    query = make_query(carrier, fn, dob)
    
    projection = {
        "flightLegState.carrier": 1,
        "flightLegState.flightNumber": 1,
        "flightLegState.dateOfOrigin": 1,
        "flightLegState.crewConnections": 1
    }
    
    return await _fetch_one_async(query, projection)

@mcp.tool()
async def raw_mongodb_query(query_json: str, projection: str = "", limit: int = 10) -> str:
    """
    Execute a raw MongoDB query (stringified JSON) with optional projection.

    Supports intelligent LLM-decided projections to reduce payload size based on query intent.

    Args:
        query_json: The MongoDB query (as stringified JSON).
        projection: Optional projection (as stringified JSON) for selecting fields.
        limit: Max number of documents to return (default 10, capped at 50).
    """

    def _safe_json_loads(text: str) -> dict:
        """Safely parse JSON, handling single quotes and formatting errors."""
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            try:
                fixed = text.replace("'", '"')
                return json.loads(fixed)
            except Exception as e:
                raise ValueError(f"Invalid JSON: {e}")

    try:
        _, _, col = await get_mongodb_client()

        # --- Parse Query ---
        try:
            query = _safe_json_loads(query_json)
        except ValueError as e:
            return response_error(f"❌ Invalid query_json: {str(e)}", 400)

        # --- Parse Projection (optional) ---
        projection_dict = None
        if projection:
            try:
                projection_dict = _safe_json_loads(projection)
            except ValueError as e:
                return response_error(f"❌ Invalid projection JSON: {str(e)}", 400)

        # --- Validate types ---
        if not isinstance(query, dict):
            return response_error("❌ query_json must be a JSON object.", 400)
        if projection_dict and not isinstance(projection_dict, dict):
            return response_error("❌ projection must be a JSON object.", 400)

        # --- Safety guard ---
        forbidden_ops = ["$where", "$out", "$merge", "$accumulator", "$function"]
        for key in query.keys():
            if key in forbidden_ops or key.startswith("$"):
                return response_error(f"❌ Operator '{key}' is not allowed.", 400)

        limit = min(max(1, int(limit)), 50)

        # --- Fallback projection ---
        # If the LLM forgets to include projection, return a minimal safe set.
        if not projection_dict:
            projection_dict = {
                "_id": 0,
                "flightLegState.carrier": 1,
                "flightLegState.flightNumber": 1,
                "flightLegState.dateOfOrigin": 1
            }
            
        logger.info(f"Executing MongoDB query: {query} | projection={projection_dict} | limit={limit}")

        # --- Run query ---
        cursor = col.find(query, projection_dict).sort("flightLegState.dateOfOrigin", -1).limit(limit)
        docs = []
        async for doc in cursor:
            doc.pop("_id", None)
            doc.pop("_class", None)
            docs.append(doc)

        if not docs:
            return response_error("No documents found for the given query.", 404)

        return response_ok({
            "count": len(docs),
            "query": query,
            "projection": projection_dict,
            "documents": docs
        })

    except Exception as exc:
        logger.exception("❌ raw_mongodb_query failed")
        return response_error(f"Raw MongoDB query failed: {str(exc)}", 500)

@mcp.tool()
async def run_aggregated_query(
    query_type: str = "",
    carrier: str = "",
    field: str = "",
    start_date: str = "",
    end_date: str = "",
    filter_json: str = ""
) -> str:
    """
    Run statistical or comparative MongoDB aggregation queries.

    Args:
        query_type: "average", "sum", "min", "max", "count".
        carrier: Optional carrier filter.
        field: Field to aggregate, e.g. "flightLegState.pax.passengerCount.count".
        start_date: Optional start date (YYYY-MM-DD).
        end_date: Optional end date (YYYY-MM-DD).
        filter_json: Optional filter query (as JSON string).
    """

    _, _, col = await get_mongodb_client()

    match_stage = {}

    # --- Optional filters ---
    if filter_json:
        try:
            match_stage.update(json.loads(filter_json.replace("'", '"')))
        except Exception as e:
            return response_error(f"Invalid filter_json: {e}", 400)

    if carrier:
        match_stage["flightLegState.carrier"] = carrier
    if start_date and end_date:
        match_stage["flightLegState.dateOfOrigin"] = {"$gte": start_date, "$lte": end_date}

    agg_map = {
        "average": {"$avg": f"${field}"},
        "sum": {"$sum": f"${field}"},
        "min": {"$min": f"${field}"},
        "max": {"$max": f"${field}"},
        "count": {"$sum": 1},
    }

    if query_type not in agg_map:
        return response_error(f"Unsupported query_type '{query_type}'. Use one of: average, sum, min, max, count", 400)

    pipeline = [{"$match": match_stage}, {"$group": {"_id": None, "value": agg_map[query_type]}}]

    try:
        logger.info(f"Running aggregation pipeline: {pipeline}")
        docs = await col.aggregate(pipeline).to_list(length=10)
        return response_ok({"pipeline": pipeline, "results": docs})
    except Exception as e:
        logger.exception("Aggregation query failed")
        return response_error(f"Aggregation failed: {str(e)}", 500)

# --- Run MCP Server ---
if __name__ == "__main__":
    logger.info("Starting FlightOps MCP Server on %s:%s (transport=%s)", HOST, PORT, TRANSPORT)
    logger.info("MongoDB URL: %s, Database: %s, Collection: %s", MONGODB_URL, DATABASE_NAME, COLLECTION_NAME)
    mcp.run(transport="streamable-http")
