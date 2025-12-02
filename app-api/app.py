from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from datetime import datetime
from typing import List

app = FastAPI(title="Business Process Tracker API", version="1.0.0")

# In-memory storage for the session
processes = {}


class MilestoneStatus(BaseModel):
    name: str
    completed: bool


class ProcessRequest(BaseModel):
    process_id: str
    process_name: str
    milestones: List[str]


class MilestoneUpdateRequest(BaseModel):
    process_id: str
    milestone_name: str
    completed: bool


@app.get("/", response_class=HTMLResponse)
def home():
    """Home endpoint showing available endpoints and usage examples."""
    # Build process table rows
    process_rows = ""
    if processes:
        for process_id, process_data in processes.items():
            completed_count = sum(1 for v in process_data["milestones"].values() if v)
            total_milestones = len(process_data["milestones"])
            
            if completed_count == 0:
                status = "Pending"
            elif completed_count < total_milestones:
                status = "Started"
            else:
                status = "Completed"
            
            process_rows += f"""
                <tr>
                    <td><code>{process_id}</code></td>
                    <td>{process_data['process_name']}</td>
                    <td><span style="padding: 4px 8px; border-radius: 4px; font-weight: 600; background: {'#90EE90' if status == 'Completed' else '#FFD700' if status == 'Started' else '#D3D3D3'}; color: #000;">{status}</span></td>
                </tr>
            """
    else:
        process_rows = "<tr><td colspan='3' style='text-align: center; color: #999;'>No processes yet</td></tr>"
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Business Process Tracker API</title>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 20px;
                background: linear-gradient(135deg, #1f77e0 0%, #0d47a1 100%);
                min-height: 100vh;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
                background: white;
                border-radius: 10px;
                box-shadow: 0 10px 40px rgba(0,0,0,0.2);
                padding: 40px;
            }}
            h1 {{
                color: #333;
                margin-bottom: 10px;
            }}
            .description {{
                color: #666;
                font-size: 16px;
                margin-bottom: 30px;
            }}
            .endpoints {{
                margin-top: 30px;
            }}
            .endpoint {{
                background: #f8f9fa;
                border-left: 4px solid #1f77e0;
                padding: 20px;
                margin-bottom: 20px;
                border-radius: 5px;
            }}
            .endpoint h3 {{
                margin-top: 0;
                color: #1f77e0;
            }}
            .method {{
                display: inline-block;
                padding: 5px 10px;
                border-radius: 4px;
                font-weight: bold;
                font-size: 12px;
                margin-right: 10px;
            }}
            .method.get {{
                background: #61affe;
                color: white;
            }}
            .method.post {{
                background: #49cc90;
                color: white;
            }}
            .method.put {{
                background: #fca130;
                color: white;
            }}
            .payload {{
                background: white;
                padding: 10px;
                border-radius: 4px;
                margin: 10px 0;
                font-family: 'Courier New', monospace;
                font-size: 13px;
            }}
            .example-code {{
                background: #1e1e1e;
                color: #d4d4d4;
                padding: 15px;
                border-radius: 5px;
                margin: 10px 0;
                overflow-x: auto;
                font-family: 'Courier New', monospace;
                font-size: 12px;
            }}
            .section-title {{
                color: #333;
                font-size: 18px;
                margin-top: 30px;
                margin-bottom: 15px;
                border-bottom: 2px solid #1f77e0;
                padding-bottom: 10px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 15px;
            }}
            th {{
                background: #1f77e0;
                color: white;
                padding: 12px;
                text-align: left;
                font-weight: 600;
            }}
            td {{
                padding: 12px;
                border-bottom: 1px solid #e0e0e0;
            }}
            tr:hover {{
                background: #f5f5f5;
            }}
            code {{
                background: #f0f0f0;
                padding: 2px 6px;
                border-radius: 3px;
                font-family: 'Courier New', monospace;
                font-size: 12px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📋 Business Process Tracker API</h1>
            <p class="description">Track and manage business process milestones. Create processes, mark milestones as complete, and monitor progress.</p>
            
            <div class="section-title">Active Processes</div>
            <table>
                <thead>
                    <tr>
                        <th>Process ID</th>
                        <th>Process Name</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {process_rows}
                </tbody>
            </table>
            
            <div class="section-title">Available Endpoints</div>
            
            <div class="endpoints">
                <div class="endpoint">
                    <h3><span class="method get">GET</span>Home</h3>
                    <p><strong>Path:</strong> <code>/</code></p>
                    <p><strong>Description:</strong> Returns this documentation page.</p>
                </div>
                
                <div class="endpoint">
                    <h3><span class="method post">POST</span>Create Process</h3>
                    <p><strong>Path:</strong> <code>/process</code></p>
                    <p><strong>Description:</strong> Create a new business process with defined milestones.</p>
                    <p><strong>Payload:</strong></p>
                    <div class="payload">{{
  "process_id": "sales-q4-2024",
  "process_name": "Q4 Sales Campaign",
  "milestones": ["Planning", "Launch", "Review", "Completion"]
}}</div>
                </div>
                
                <div class="endpoint">
                    <h3><span class="method get">GET</span>Get Process</h3>
                    <p><strong>Path:</strong> <code>/process/{{process_id}}</code></p>
                    <p><strong>Description:</strong> Retrieve a process and its milestone statuses.</p>
                    <p><strong>Example:</strong> <code>/process/sales-q4-2024</code></p>
                </div>
                
                <div class="endpoint">
                    <h3><span class="method put">PUT</span>Update Milestone</h3>
                    <p><strong>Path:</strong> <code>/milestone</code></p>
                    <p><strong>Description:</strong> Mark a milestone as completed or revert it.</p>
                    <p><strong>Payload:</strong></p>
                    <div class="payload">{{
  "process_id": "sales-q4-2024",
  "milestone_name": "Planning",
  "completed": true
}}</div>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return html_content


@app.post("/process")
def create_process(request: ProcessRequest):
    """Create a new business process with milestones."""
    if request.process_id in processes:
        return {"error": f"Process {request.process_id} already exists"}
    
    processes[request.process_id] = {
        "process_name": request.process_name,
        "created_at": datetime.now().isoformat(),
        "milestones": {milestone: False for milestone in request.milestones}
    }
    
    return {
        "message": f"Process {request.process_id} created successfully",
        "process": processes[request.process_id]
    }


@app.get("/process/{process_id}")
def get_process(process_id: str):
    """Get a specific process and its milestone statuses."""
    if process_id not in processes:
        return {"error": f"Process {process_id} not found"}
    
    process_data = processes[process_id]
    completed_count = sum(1 for v in process_data["milestones"].values() if v)
    total_milestones = len(process_data["milestones"])
    
    return {
        "process_id": process_id,
        "process_name": process_data["process_name"],
        "created_at": process_data["created_at"],
        "progress_percent": round((completed_count / total_milestones * 100), 1) if total_milestones > 0 else 0,
        "milestones": process_data["milestones"],
        "completed_milestones": completed_count,
        "total_milestones": total_milestones
    }


@app.put("/milestone")
def update_milestone(request: MilestoneUpdateRequest):
    """Update the completion status of a milestone."""
    if request.process_id not in processes:
        return {"error": f"Process {request.process_id} not found"}
    
    if request.milestone_name not in processes[request.process_id]["milestones"]:
        return {"error": f"Milestone {request.milestone_name} not found"}
    
    processes[request.process_id]["milestones"][request.milestone_name] = request.completed
    
    return {
        "message": f"Milestone {request.milestone_name} updated",
        "process_id": request.process_id,
        "milestone_name": request.milestone_name,
        "completed": request.completed,
        "process": get_process(request.process_id)
    }
