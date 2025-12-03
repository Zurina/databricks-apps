from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Dict

app = FastAPI(title="Simple Topic API", version="1.0.0")

# In-memory storage for topics
topics: Dict[str, Dict[str, str]] = {}


class TopicRequest(BaseModel):
    id: str
    title: str
    content: str = ""


@app.get("/", response_class=HTMLResponse)
def home():
    """Home endpoint showing created topics and simple API docs."""
    if topics:
        rows = "".join(
            f"<tr><td><code>{tid}</code></td><td>{t['title']}</td><td>{t['content']}</td></tr>"
            for tid, t in topics.items()
        )
    else:
        rows = "<tr><td colspan='3' style='text-align:center;color:#666;'>No topics yet</td></tr>"

    sample_payload = '{"id": "topic-1", "title": "My First Topic", "content": "A short description"}'

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <title>Simple Topic API</title>
      <style>
        body {{ font-family: Arial, sans-serif; padding: 24px; background: #f7f9fc; }}
        .container {{ max-width: 900px; margin: 0 auto; background: #fff; padding: 24px; border-radius: 8px; box-shadow: 0 6px 20px rgba(0,0,0,0.08); }}
        table {{ width:100%; border-collapse: collapse; margin-top: 16px; }}
        th, td {{ padding: 10px; border-bottom: 1px solid #eee; text-align: left; }}
        th {{ background: #0d6efd; color: #fff; }}
        pre {{ background: #f1f3f5; padding: 12px; border-radius: 6px; overflow-x:auto; }}
      </style>
    </head>
    <body>
      <div class="container">
        <h1>Simple Topic API</h1>
        <p>Create simple topics via the <code>POST /topic</code> endpoint and view them here.</p>

        <h3>Active Topics</h3>
        <table>
          <thead>
            <tr><th>ID</th><th>Title</th><th>Content</th></tr>
          </thead>
          <tbody>
            {rows}
          </tbody>
        </table>

        <h3>Endpoints</h3>
        <ul>
          <li><strong>POST /topic</strong> — Create a topic. Payload example shown below.</li>
          <li><strong>GET /topics</strong> — List all topics (JSON).</li>
          <li><strong>GET /topic/&lt;id&gt;</strong> — Get a topic by id (JSON).</li>
        </ul>

        <h4>Example POST payload</h4>
        <pre>{sample_payload}</pre>
      </div>
    </body>
    </html>
    """

    return html


@app.post("/topic")
def create_topic(request: TopicRequest):
    """Create a new topic."""
    if request.id in topics:
        return {"error": f"Topic {request.id} already exists"}

    topics[request.id] = {"title": request.title, "content": request.content}
    return {"message": f"Topic {request.id} created", "topic": {"id": request.id, **topics[request.id]}}


@app.get("/topic/{topic_id}")
def get_topic(topic_id: str):
    """Retrieve a topic by id."""
    if topic_id not in topics:
        return {"error": f"Topic {topic_id} not found"}
    return {"id": topic_id, **topics[topic_id]}


@app.get("/topics")
def list_topics():
    """Return all topics as JSON."""
    return {"topics": [{"id": tid, **t} for tid, t in topics.items()]}
