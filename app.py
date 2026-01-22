import os
import glob
import time
import uuid
import subprocess
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

POSTERS_DIR = "posters"
os.makedirs(POSTERS_DIR, exist_ok=True)

def list_themes():
    files = sorted(glob.glob("themes/*.json"))
    return [os.path.splitext(os.path.basename(f))[0] for f in files]

class GenerateRequest(BaseModel):
    city: str = Field(..., min_length=1, max_length=80)
    country: str = Field(..., min_length=1, max_length=80)
    theme: str = Field(..., min_length=1, max_length=80)
    distance: int = Field(8000, ge=1000, le=20000)

app = FastAPI(title="MapToPoster API")

allowed = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in allowed] if allowed != ["*"] else ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True, "themes": list_themes()}

def newest_png():
    files = glob.glob(os.path.join(POSTERS_DIR, "*.png"))
    if not files:
        return None
    return max(files, key=os.path.getmtime)

@app.post("/generate")
def generate(req: GenerateRequest):
    themes = list_themes()
    if req.theme not in themes:
        raise HTTPException(status_code=400, detail=f"Theme invalid. Available: {themes}")

    city = req.city.strip()
    country = req.country.strip()

    before = newest_png()

    cmd = [
        "python",
        "create_map_poster.py",
        "--city", city,
        "--country", country,
        "--theme", req.theme,
        "--distance", str(int(req.distance)),
    ]

    try:
        subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=80)
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Generation timeout.")
    except subprocess.CalledProcessError as e:
        msg = (e.stderr or e.stdout or "Generation failed").strip()
        raise HTTPException(status_code=500, detail=msg)

    time.sleep(0.2)
    after = newest_png()
    if not after or after == before:
        raise HTTPException(status_code=500, detail="Poster not found in posters/ after generation.")

    out_id = uuid.uuid4().hex[:10]
    target = os.path.join(POSTERS_DIR, f"{out_id}.png")
    os.replace(after, target)

    return FileResponse(target, media_type="image/png", filename=f"{city}_{req.theme}.png")
