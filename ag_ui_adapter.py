# # ag_ui_adapter.py
# import os
# import json
# import asyncio
# import time
# import uuid
# from typing import AsyncGenerator, List
# from fastapi import FastAPI, Request, HTTPException
# from fastapi.responses import StreamingResponse
# from fastapi.middleware.cors import CORSMiddleware
# from client import FlightOpsMCPClient

# app = FastAPI(title="FlightOps — AG-UI Adapter")

# # CORS (adjust origins for your Vite origin)
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],           # lock down in prod
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# mcp_client = FlightOpsMCPClient()

# def sse_event(data: dict) -> str:
#     """Encode one SSE event (JSON payload)"""
#     return f"data: {json.dumps(data, default=str, ensure_ascii=False)}\n\n"

# async def ensure_mcp_connected():
#     if not mcp_client.session:
#         await mcp_client.connect()

# @app.on_event("startup")
# async def startup_event():
#     try:
#         await ensure_mcp_connected()
#     except Exception:
#         # don't crash; /health will reflect status
#         pass

# @app.get("/")
# async def root():
#     return {"message": "FlightOps AG-UI Adapter running", "status": "ok"}

# @app.get("/health")
# async def health():
#     try:
#         await ensure_mcp_connected()
#         return {"status": "healthy", "mcp_connected": True}
#     except Exception as e:
#         return {"status": "unhealthy", "mcp_connected": False, "error": str(e)}

# def chunk_text(txt: str, max_len: int = 200) -> List[str]:
#     """
#     Split text into small chunks for streaming as typing.
#     Prefer sentence boundaries; fallback by length.
#     """
#     txt = txt or ""
#     parts: List[str] = []
#     buf = ""

#     def flush():
#         nonlocal buf
#         if buf:
#             parts.append(buf)
#             buf = ""

#     for ch in txt:
#         buf += ch
#         # flush at sentence end or when too long
#         if ch in ".!?\n" and len(buf) >= max_len // 2:
#             flush()
#         elif len(buf) >= max_len:
#             flush()
#     flush()
#     return parts

# @app.post("/agent", response_class=StreamingResponse)
# async def run_agent(request: Request):
#     """
#     AG-UI compatible streaming endpoint (SSE).
#     Expected body:
#       {
#         thread_id?, run_id?,
#         messages: [{role, content}, ...],
#         tools?: []
#       }
#     """
#     try:
#         body = await request.json()
#     except Exception:
#         raise HTTPException(status_code=400, detail="Invalid JSON body")

#     thread_id = body.get("thread_id") or f"thread-{uuid.uuid4().hex[:8]}"
#     run_id = body.get("run_id") or f"run-{uuid.uuid4().hex[:8]}"
#     messages = body.get("messages", [])

#     # last user message as query
#     user_query = ""
#     if messages:
#         last = messages[-1]
#         if isinstance(last, dict) and last.get("role") == "user":
#             user_query = last.get("content", "") or ""
#         elif isinstance(last, str):
#             user_query = last

#     if not user_query.strip():
#         raise HTTPException(status_code=400, detail="No user query found")

#     async def event_stream() -> AsyncGenerator[str, None]:
#         last_heartbeat = time.time()

#         # --- RUN STARTED
#         yield sse_event({"type": "RUN_STARTED", "thread_id": thread_id, "run_id": run_id})
        
#         # THINKING: Initial analysis
#         yield sse_event({
#             "type": "STATE_UPDATE", 
#             "state": {
#                 "phase": "thinking", 
#                 "progress_pct": 5,
#                 "message": "🧠 Analyzing your flight query..."
#             }
#         })

#         # ensure MCP
#         try:
#             await ensure_mcp_connected()
#         except Exception as e:
#             yield sse_event({"type": "RUN_ERROR", "error": f"MCP connect failed: {e}"})
#             return

#         loop = asyncio.get_event_loop()

#         # --- PLAN (THINKING phase)
#         yield sse_event({
#             "type": "STATE_UPDATE", 
#             "state": {
#                 "phase": "thinking", 
#                 "progress_pct": 15,
#                 "message": "📋 Planning which flight data tools to use..."
#             }
#         })
        
#         try:
#             plan_data = await loop.run_in_executor(None, mcp_client.plan_tools, user_query)
#         except Exception as e:
#             yield sse_event({"type": "RUN_ERROR", "error": f"Planner error: {e}"})
#             return

#         plan = plan_data.get("plan", []) if isinstance(plan_data, dict) else []
#         yield sse_event({"type": "STATE_SNAPSHOT", "snapshot": {"plan": plan}})

#         if not plan:
#             yield sse_event({
#                 "type": "TEXT_MESSAGE_CONTENT", 
#                 "message": {
#                     "id": f"msg-{uuid.uuid4().hex[:8]}",
#                     "role": "assistant",
#                     "content": "I couldn't generate a valid plan for your query. Please try rephrasing."
#                 }
#             })
#             yield sse_event({"type": "STATE_UPDATE", "state": {"phase": "finished", "progress_pct": 100}})
#             yield sse_event({"type": "RUN_FINISHED", "run_id": run_id})
#             return

#         # --- PROCESSING: Tool execution
#         yield sse_event({
#             "type": "STATE_UPDATE", 
#             "state": {
#                 "phase": "processing", 
#                 "progress_pct": 20,
#                 "message": f"🛠️ Executing {len(plan)} flight data tools..."
#             }
#         })

#         results = []
#         num_steps = max(1, len(plan))
#         per_step = 60.0 / num_steps  # 20% to 80%
#         current_progress = 20.0

#         for step_index, step in enumerate(plan):
#             if await request.is_disconnected():
#                 return

#             tool_name = step.get("tool")
#             args = step.get("arguments", {}) or {}

#             # Update processing status for current tool
#             yield sse_event({
#                 "type": "STATE_UPDATE", 
#                 "state": {
#                     "phase": "processing",
#                     "progress_pct": round(current_progress),
#                     "message": f"🔧 Running {tool_name}..."
#                 }
#             })

#             tool_call_id = f"toolcall-{uuid.uuid4().hex[:8]}"
            
#             # Tool call events
#             yield sse_event({
#                 "type": "TOOL_CALL_START",
#                 "toolCallId": tool_call_id,
#                 "toolCallName": tool_name,
#                 "parentMessageId": None
#             })

#             yield sse_event({
#                 "type": "TOOL_CALL_ARGS",
#                 "toolCallId": tool_call_id,
#                 "delta": json.dumps(args, ensure_ascii=False)
#             })
#             yield sse_event({"type": "TOOL_CALL_END", "toolCallId": tool_call_id})

#             # call tool
#             try:
#                 tool_result = await mcp_client.invoke_tool(tool_name, args)
#             except Exception as exc:
#                 tool_result = {"error": str(exc)}

#             # result
#             yield sse_event({
#                 "type": "TOOL_CALL_RESULT",
#                 "message": {
#                     "id": f"msg-{uuid.uuid4().hex[:8]}",
#                     "role": "tool",
#                     "content": json.dumps(tool_result, ensure_ascii=False),
#                     "tool_call_id": tool_call_id,
#                 }
#             })
#             results.append({tool_name: tool_result})

#             yield sse_event({
#                 "type": "STEP_FINISHED",
#                 "step_index": step_index,
#                 "tool": tool_name
#             })

#             # update progress
#             current_progress = min(80.0, 20.0 + per_step * (step_index + 1))
            
#             # heartbeat every ~15s while long tools run
#             if time.time() - last_heartbeat > 15:
#                 yield sse_event({"type": "HEARTBEAT", "ts": time.time()})
#                 last_heartbeat = time.time()

#         # --- TYPING: Result generation
#         yield sse_event({
#             "type": "STATE_UPDATE", 
#             "state": {
#                 "phase": "typing", 
#                 "progress_pct": 85,
#                 "message": "✍️ Generating your flight analysis..."
#             }
#         })

#         try:
#             summary_obj = await loop.run_in_executor(None, mcp_client.summarize_results, user_query, plan, results)
#             assistant_text = summary_obj.get("summary", "") if isinstance(summary_obj, dict) else str(summary_obj)
#         except Exception as e:
#             assistant_text = f"❌ Failed to summarize results: {e}"

#         # Stream summary as chunks (typing effect)
#         msg_id = f"msg-{uuid.uuid4().hex[:8]}"
        
#         # Start the message
#         yield sse_event({
#             "type": "TEXT_MESSAGE_CONTENT",
#             "message": {
#                 "id": msg_id,
#                 "role": "assistant",
#                 "content": ""  # Start with empty content
#             }
#         })

#         # Stream chunks with typing effect
#         chunks = chunk_text(assistant_text, max_len=150)
#         for i, chunk in enumerate(chunks):
#             yield sse_event({
#                 "type": "TEXT_MESSAGE_CONTENT",
#                 "message": {
#                     "id": msg_id,
#                     "role": "assistant", 
#                     "delta": chunk  # AG-UI delta for streaming
#                 }
#             })
            
#             # Update typing progress
#             typing_progress = 85 + (i / len(chunks)) * 15
#             yield sse_event({
#                 "type": "STATE_UPDATE",
#                 "state": {
#                     "phase": "typing",
#                     "progress_pct": round(typing_progress),
#                     "message": "✍️ Generating your flight analysis..."
#                 }
#             })
            
#             await asyncio.sleep(0.03)  # Typing speed

#         # Final state
#         yield sse_event({"type": "STATE_SNAPSHOT", "snapshot": {"plan": plan, "results": results}})
#         yield sse_event({
#             "type": "STATE_UPDATE", 
#             "state": {
#                 "phase": "finished", 
#                 "progress_pct": 100,
#                 "message": "✅ Analysis complete"
#             }
#         })
#         yield sse_event({"type": "RUN_FINISHED", "run_id": run_id})

#     return StreamingResponse(event_stream(), media_type="text/event-stream")
#####################################################################
import os
import json
import asyncio
import time
import uuid
import re
from typing import AsyncGenerator, List
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from client import FlightOpsMCPClient

app = FastAPI(title="FlightOps — AG-UI Adapter")

# CORS (adjust origins for your Vite origin)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

mcp_client = FlightOpsMCPClient()

# Global context storage for route selection
route_selection_context = {}

def sse_event(data: dict) -> str:
    """Encode one SSE event (JSON payload)"""
    return f"data: {json.dumps(data, default=str, ensure_ascii=False)}\n\n"

async def ensure_mcp_connected():
    if not mcp_client.session:
        await mcp_client.connect()

@app.on_event("startup")
async def startup_event():
    try:
        await ensure_mcp_connected()
    except Exception:
        pass

@app.get("/")
async def root():
    return {"message": "FlightOps AG-UI Adapter running", "status": "ok"}

@app.get("/health")
async def health():
    try:
        await ensure_mcp_connected()
        return {"status": "healthy", "mcp_connected": True}
    except Exception as e:
        return {"status": "unhealthy", "mcp_connected": False, "error": str(e)}

def chunk_text(txt: str, max_len: int = 200) -> List[str]:
    txt = txt or ""
    parts: List[str] = []
    buf = ""

    def flush():
        nonlocal buf
        if buf:
            parts.append(buf)
            buf = ""

    for ch in txt:
        buf += ch
        if ch in ".!?\n" and len(buf) >= max_len // 2:
            flush()
        elif len(buf) >= max_len:
            flush()
    flush()
    return parts

@app.post("/agent", response_class=StreamingResponse)
async def run_agent(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    thread_id = body.get("thread_id") or f"thread-{uuid.uuid4().hex[:8]}"
    run_id = body.get("run_id") or f"run-{uuid.uuid4().hex[:8]}"
    messages = body.get("messages", [])
    
    # Check if this is a route selection response
    is_route_selection = False
    user_selected_route = None
    original_tool_args = None
    
    if thread_id in route_selection_context:
        # This is a follow-up message for route selection
        last_message = messages[-1] if messages else {}
        if isinstance(last_message, dict) and last_message.get("role") == "user":
            user_input = last_message.get("content", "").strip()
            is_route_selection = True
            context = route_selection_context[thread_id]
            available_routes = context.get("available_routes", [])
            original_tool_args = context.get("original_args", {})
            
            # Parse user's route selection
            try:
                # Extract number from input (handle "1", "option 1", "route 1", etc.)
                numbers = re.findall(r'\d+', user_input)
                if numbers:
                    route_number = int(numbers[0])
                    if 1 <= route_number <= len(available_routes):
                        selected_route = available_routes[route_number - 1]["route_id"]
                        user_selected_route = selected_route
                    else:
                        # Invalid route number
                        user_selected_route = "invalid"
                else:
                    # No number found
                    user_selected_route = "invalid"
            except:
                user_selected_route = "invalid"

    # Get user query
    user_query = ""
    if messages:
        last = messages[-1]
        if isinstance(last, dict) and last.get("role") == "user":
            user_query = last.get("content", "") or ""
        elif isinstance(last, str):
            user_query = last

    async def event_stream() -> AsyncGenerator[str, None]:
        last_heartbeat = time.time()

        # --- RUN STARTED
        yield sse_event({"type": "RUN_STARTED", "thread_id": thread_id, "run_id": run_id})
        
        # --- CASE 1: Route Selection Response
        if is_route_selection:
            if user_selected_route == "invalid":
                # Invalid selection - show error and ask again
                yield sse_event({
                    "type": "TEXT_MESSAGE_CONTENT",
                    "message": {
                        "id": f"msg-{uuid.uuid4().hex[:8]}",
                        "role": "assistant",
                        "content": "❌ Invalid selection. Please enter only the route number (1, 2, 3, etc.)."
                    }
                })
                yield sse_event({"type": "STATE_UPDATE", "state": {"phase": "finished", "progress_pct": 100}})
                yield sse_event({"type": "RUN_FINISHED", "run_id": run_id})
                return
            
            # Valid route selected - process the actual query
            yield sse_event({
                "type": "STATE_UPDATE", 
                "state": {
                    "phase": "processing", 
                    "progress_pct": 50,
                    "message": f"🔄 Fetching delay summary for route {user_selected_route}..."
                }
            })
            
            try:
                # Call the tool again with selected route
                tool_result = await mcp_client.invoke_tool(
                    "get_delay_summary", 
                    {**original_tool_args, "selected_route": user_selected_route}
                )
                
                # Clear the context
                route_selection_context.pop(thread_id, None)
                
                # Process and display the actual delay summary
                if isinstance(tool_result, dict) and tool_result.get("ok"):
                    delay_data = tool_result['data']
                    
                    # Format the delay summary nicely
                    summary_parts = []
                    summary_parts.append(f"**✈️ Delay Summary for {user_selected_route}**")
                    summary_parts.append(f"**Flight:** {delay_data.get('flightLegState', {}).get('carrier', '')}{delay_data.get('flightLegState', {}).get('flightNumber', '')}")
                    summary_parts.append(f"**Date:** {delay_data.get('flightLegState', {}).get('dateOfOrigin', '')}")
                    summary_parts.append(f"**Route:** {delay_data.get('flightLegState', {}).get('startStation', '')} → {delay_data.get('flightLegState', {}).get('endStation', '')}")
                    summary_parts.append(f"**Status:** {delay_data.get('flightLegState', {}).get('flightStatus', 'N/A')}")
                    
                    delays = delay_data.get('flightLegState', {}).get('delays', {})
                    if delays:
                        summary_parts.append("\n**Delays:**")
                        if isinstance(delays, list):
                            for delay in delays:
                                if isinstance(delay, dict):
                                    summary_parts.append(f"- Reason: {delay.get('reason', 'Unknown')}, Duration: {delay.get('duration', 'N/A')} minutes")
                        elif isinstance(delays, dict):
                            for key, value in delays.items():
                                summary_parts.append(f"- {key}: {value}")
                    
                    formatted_summary = "\n".join(summary_parts)
                    
                    yield sse_event({
                        "type": "TEXT_MESSAGE_CONTENT",
                        "message": {
                            "id": f"msg-{uuid.uuid4().hex[:8]}",
                            "role": "assistant",
                            "content": formatted_summary
                        }
                    })
                else:
                    yield sse_event({
                        "type": "TEXT_MESSAGE_CONTENT",
                        "message": {
                            "id": f"msg-{uuid.uuid4().hex[:8]}",
                            "role": "assistant", 
                            "content": f"❌ Error fetching delay summary: {tool_result}"
                        }
                    })
                    
            except Exception as e:
                yield sse_event({
                    "type": "TEXT_MESSAGE_CONTENT", 
                    "message": {
                        "id": f"msg-{uuid.uuid4().hex[:8]}",
                        "role": "assistant",
                        "content": f"❌ Error processing route selection: {str(e)}"
                    }
                })
            
            yield sse_event({"type": "STATE_UPDATE", "state": {"phase": "finished", "progress_pct": 100}})
            yield sse_event({"type": "RUN_FINISHED", "run_id": run_id})
            return

        # --- CASE 2: New Query (Normal Flow)
        if not user_query.strip():
            raise HTTPException(status_code=400, detail="No user query found")

        # THINKING: Initial analysis
        yield sse_event({
            "type": "STATE_UPDATE", 
            "state": {
                "phase": "thinking", 
                "progress_pct": 5,
                "message": "🧠 Analyzing your flight query..."
            }
        })

        # ensure MCP
        try:
            await ensure_mcp_connected()
        except Exception as e:
            yield sse_event({"type": "RUN_ERROR", "error": f"MCP connect failed: {e}"})
            return

        loop = asyncio.get_event_loop()

        # --- PLAN (THINKING phase)
        yield sse_event({
            "type": "STATE_UPDATE", 
            "state": {
                "phase": "thinking", 
                "progress_pct": 15,
                "message": "📋 Planning which flight data tools to use..."
            }
        })
        
        try:
            plan_data = await loop.run_in_executor(None, mcp_client.plan_tools, user_query)
        except Exception as e:
            yield sse_event({"type": "RUN_ERROR", "error": f"Planner error: {e}"})
            return

        plan = plan_data.get("plan", []) if isinstance(plan_data, dict) else []
        planning_usage = plan_data.get("llm_usage", {})
        
        # DEBUG: Send token usage for planning
        print(f"DEBUG: Planning token usage: {planning_usage}")
        if planning_usage:
            yield sse_event({
                "type": "TOKEN_USAGE",
                "phase": "planning",
                "usage": planning_usage
            })
        
        yield sse_event({"type": "STATE_SNAPSHOT", "snapshot": {"plan": plan}})

        if not plan:
            yield sse_event({
                "type": "TEXT_MESSAGE_CONTENT", 
                "message": {
                    "id": f"msg-{uuid.uuid4().hex[:8]}",
                    "role": "assistant",
                    "content": "I couldn't generate a valid plan for your query. Please try rephrasing."
                }
            })
            yield sse_event({"type": "STATE_UPDATE", "state": {"phase": "finished", "progress_pct": 100}})
            yield sse_event({"type": "RUN_FINISHED", "run_id": run_id})
            return

        # --- PROCESSING: Tool execution
        yield sse_event({
            "type": "STATE_UPDATE", 
            "state": {
                "phase": "processing", 
                "progress_pct": 20,
                "message": f"🛠️ Executing {len(plan)} flight data tools..."
            }
        })

        results = []
        num_steps = max(1, len(plan))
        per_step = 60.0 / num_steps
        current_progress = 20.0

        for step_index, step in enumerate(plan):
            if await request.is_disconnected():
                return

            tool_name = step.get("tool")
            args = step.get("arguments", {}) or {}

            yield sse_event({
                "type": "STATE_UPDATE", 
                "state": {
                    "phase": "processing",
                    "progress_pct": round(current_progress),
                    "message": f"🔧 Running {tool_name}..."
                }
            })

            tool_call_id = f"toolcall-{uuid.uuid4().hex[:8]}"
            
            yield sse_event({
                "type": "TOOL_CALL_START",
                "toolCallId": tool_call_id,
                "toolCallName": tool_name,
                "parentMessageId": None
            })

            yield sse_event({
                "type": "TOOL_CALL_ARGS",
                "toolCallId": tool_call_id,
                "delta": json.dumps(args, ensure_ascii=False)
            })
            yield sse_event({"type": "TOOL_CALL_END", "toolCallId": tool_call_id})

            try:
                tool_result = await mcp_client.invoke_tool(tool_name, args)
                
                # Check if multiple routes are available (for delay summary)
                if (tool_name == "get_delay_summary" and 
                    isinstance(tool_result, dict) and 
                    tool_result.get("ok") and 
                    tool_result.get("data", {}).get("status") == "route_selection_required"):
                    
                    # Store context for route selection
                    route_data = tool_result["data"]
                    route_selection_context[thread_id] = {
                        "available_routes": route_data["available_routes"],
                        "original_args": args
                    }
                    
                    # Format route options for display
                    route_options = []
                    for idx, route in enumerate(route_data["available_routes"]):
                        route_options.append(
                            f"**{idx+1}. {route['startStation']} → {route['endStation']}**\n"
                            f"   - Time: {route.get('scheduledStartTime', 'Unknown')}\n"
                            f"   - Status: {route['flightStatus']}"
                        )
                    
                    route_message = "\n\n".join(route_options)
                    
                    yield sse_event({
                        "type": "TEXT_MESSAGE_CONTENT",
                        "message": {
                            "id": f"msg-{uuid.uuid4().hex[:8]}",
                            "role": "assistant",
                            "content": f"**🛫 Multiple Routes Found!**\n\n"
                                      f"{route_message}\n\n"
                                      f"**Please select a route by entering the number (1-{len(route_data['available_routes'])}):**"
                        }
                    })
                    
                    # Stop further processing - wait for user route selection
                    yield sse_event({"type": "STATE_UPDATE", "state": {"phase": "finished", "progress_pct": 100}})
                    yield sse_event({"type": "RUN_FINISHED", "run_id": run_id})
                    return
                    
            except Exception as exc:
                tool_result = {"error": str(exc)}

            yield sse_event({
                "type": "TOOL_CALL_RESULT",
                "message": {
                    "id": f"msg-{uuid.uuid4().hex[:8]}",
                    "role": "tool",
                    "content": json.dumps(tool_result, ensure_ascii=False),
                    "tool_call_id": tool_call_id,
                }
            })
            results.append({tool_name: tool_result})

            yield sse_event({
                "type": "STEP_FINISHED",
                "step_index": step_index,
                "tool": tool_name
            })

            current_progress = min(80.0, 20.0 + per_step * (step_index + 1))
            
            if time.time() - last_heartbeat > 15:
                yield sse_event({"type": "HEARTBEAT", "ts": time.time()})
                last_heartbeat = time.time()

        # --- TYPING: Result generation
        yield sse_event({
            "type": "STATE_UPDATE", 
            "state": {
                "phase": "typing", 
                "progress_pct": 85,
                "message": "✍️ Generating your flight analysis..."
            }
        })

        try:
            summary_data = await loop.run_in_executor(None, mcp_client.summarize_results, user_query, plan, results)
            assistant_text = summary_data.get("summary", "") if isinstance(summary_data, dict) else str(summary_data)
            summarization_usage = summary_data.get("llm_usage", {})
            
            # DEBUG: Send token usage for summarization
            print(f"DEBUG: Summarization token usage: {summarization_usage}")
            if summarization_usage:
                yield sse_event({
                    "type": "TOKEN_USAGE",
                    "phase": "summarization", 
                    "usage": summarization_usage
                })
                
                # Calculate and send total token usage
                def safe_int(val):
                    return val if isinstance(val, int) else 0
                    
                total_usage = {
                    "prompt_tokens": safe_int(planning_usage.get('prompt_tokens', 0)) + safe_int(summarization_usage.get('prompt_tokens', 0)),
                    "completion_tokens": safe_int(planning_usage.get('completion_tokens', 0)) + safe_int(summarization_usage.get('completion_tokens', 0)),
                    "total_tokens": safe_int(planning_usage.get('total_tokens', 0)) + safe_int(summarization_usage.get('total_tokens', 0))
                }
                
                print(f"DEBUG: Total token usage: {total_usage}")
                yield sse_event({
                    "type": "TOKEN_USAGE",
                    "phase": "total",
                    "usage": total_usage
                })
                
        except Exception as e:
            assistant_text = f"❌ Failed to summarize results: {e}"

        # Stream summary as chunks
        msg_id = f"msg-{uuid.uuid4().hex[:8]}"
        
        yield sse_event({
            "type": "TEXT_MESSAGE_CONTENT",
            "message": {
                "id": msg_id,
                "role": "assistant",
                "content": ""
            }
        })

        chunks = chunk_text(assistant_text, max_len=150)
        for i, chunk in enumerate(chunks):
            yield sse_event({
                "type": "TEXT_MESSAGE_CONTENT",
                "message": {
                    "id": msg_id,
                    "role": "assistant", 
                    "delta": chunk
                }
            })
            
            typing_progress = 85 + (i / len(chunks)) * 15
            yield sse_event({
                "type": "STATE_UPDATE",
                "state": {
                    "phase": "typing",
                    "progress_pct": round(typing_progress),
                    "message": "✍️ Generating your flight analysis..."
                }
            })
            
            await asyncio.sleep(0.03)

        # Final state
        yield sse_event({"type": "STATE_SNAPSHOT", "snapshot": {"plan": plan, "results": results}})
        yield sse_event({
            "type": "STATE_UPDATE", 
            "state": {
                "phase": "finished", 
                "progress_pct": 100,
                "message": "✅ Analysis complete"
            }
        })
        yield sse_event({"type": "RUN_FINISHED", "run_id": run_id})

    return StreamingResponse(event_stream(), media_type="text/event-stream")
