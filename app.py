import os
import glob
import time
import uuid
import json
import hashlib
import threading
import subprocess
from typing import Optional, Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

POSTERS_DIR = "posters"
CACHE_DIR = "cache"
JOBS_DIR = "jobs"

os.makedirs(POSTERS_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(JOBS_DIR, exist_ok=True)

# ---- Helpers ----

def list_themes():
    files = sorted(glob.glob("themes/*.json"))
    return [os.path.splitext(os.path.basename(f))[0] for f in files]

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def cache_key(city: str, country: str, theme: str, distance: int) -> str:
    payload = json.dumps(
        {"city": city.strip(), "country": country.strip(), "theme": theme.strip(), "distance": int(distance)},
        sort_keys=True
    )
    return sha1(payload)

def cache_png_path(key: str) -> str:
    return os.path.join(CACHE_DIR, f"{key}.png")

def job_path(job_id: str) -> str:
    return os.path.join(JOBS_DIR, f"{job_id}.json")

def write_job(job_id: str, data: Dict[str, Any]) -> None:
    with open(job_path(job_id), "w", encoding="utf-8") as f:
        json.dump(data, f)

def read_job(job_id: str) -> Dict[str, Any]:
    p = job_path(job_id)
    if not os.path.exists(p):
        raise HTTPException(status_code=404, detail="Job not found")
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def newest_png_in_posters() -> Optional[str]:
    files = glob.glob(os.path.join(POSTERS_DIR, "*.png"))
    if not files:
        return None
    return max(files, key=os.path.getmtime)

# ---- API ----

class GenerateRequest(BaseModel):
    city: str = Field(..., min_length=1, max_length=80)
    country: str = Field(..., min_length=1, max_length=80)
    theme: str = Field(..., min_length=1, max_length=80)
    # Render free: keep this small to reduce failures
    distance: int = Field(2000, ge=1000, le=4000)

app = FastAPI(title="MapToPoster API (Async)")

allowed = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in allowed] if allowed != ["*"] else ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"status": "ok", "message": "Use /health, /generate_async, /job/{id}, /download/{id}"}

@app.get("/health")
def health():
    return {"ok": True, "themes": list_themes()}

# In-memory lock to avoid spawning too many threads at once on free tier
JOB_LOCK = threading.Semaphore(1)

def _run_generate(job_id: str, req: GenerateRequest, key: str) -> None:
    # Pastikan hanya 1 job berat jalan di free tier
    acquired = JOB_LOCK.acquire(timeout=1)
    if not acquired:
        time.sleep(1)

    try:
        write_job(job_id, {
            "job_id": job_id,
            "status": "RUNNING",
            "created_at": time.time(),
            "cache_key": key,
            "message": "Generating..."
        })

        cached = cache_png_path(key)
        if os.path.exists(cached):
            write_job(job_id, {
                "job_id": job_id,
                "status": "DONE",
                "created_at": time.time(),
                "cache_key": key,
                "result": "/download/" + job_id,
                "message": "Served from cache"
            })
            return

        before = newest_png_in_posters()

        cmd = [
            "python",
            "create_map_poster.py",
            "--city", req.city.strip(),
            "--country", req.country.strip(),
            "--theme", req.theme.strip(),
            "--distance", str(int(req.distance)),
        ]

        # Coba generate utama
        res = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=240
        )

        # Kalau gagal, coba fallback distance lebih kecil
        if res.returncode != 0:
            if int(req.distance) > 2000:
                cmd_fallback = cmd.copy()
                cmd_fallback[-1] = "2000"

                res2 = subprocess.run(
                    cmd_fallback,
                    capture_output=True,
                    text=True,
                    timeout=240
                )

                if res2.returncode != 0:
                    msg = (res2.stderr or res2.stdout or "Generation failed").strip()
                    write_job(job_id, {
                        "job_id": job_id,
                        "status": "ERROR",
                        "created_at": time.time(),
                        "cache_key": key,
                        "message": msg[:2000]
                    })
                    return
            else:
                msg = (res.stderr or res.stdout or "Generation failed").strip()
                write_job(job_id, {
                    "job_id": job_id,
                    "status": "ERROR",
                    "created_at": time.time(),
                    "cache_key": key,
                    "message": msg[:2000]
                })
                return

        time.sleep(0.5)
        after = newest_png_in_posters()

        if not after or after == before:
            write_job(job_id, {
                "job_id": job_id,
                "status": "ERROR",
                "created_at": time.time(),
                "cache_key": key,
                "message": "Poster not found after generation."
            })
            return

        os.replace(after, cached)

        write_job(job_id, {
            "job_id": job_id,
            "status": "DONE",
            "created_at": time.time(),
            "cache_key": key,
            "result": "/download/" + job_id,
            "message": "Done"
        })

    except subprocess.TimeoutExpired:
        write_job(job_id, {
            "job_id": job_id,
            "status": "ERROR",
            "created_at": time.time(),
            "cache_key": key,
            "message": "Generation timed out."
        })

    except Exception as e:
        write_job(job_id, {
            "job_id": job_id,
            "status": "ERROR",
            "created_at": time.time(),
            "cache_key": key,
            "message": f"Unexpected error: {str(e)}"
        })

    finally:
        try:
            JOB_LOCK.release()
        except Exception:
            pass


@app.post("/generate_async")
def generate_async(req: GenerateRequest):
    themes = list_themes()
    if req.theme not in themes:
        raise HTTPException(status_code=400, detail=f"Theme invalid. Available: {themes}")

    key = cache_key(req.city, req.country, req.theme, req.distance)
    cached = cache_png_path(key)

    job_id = uuid.uuid4().hex[:10]

    # If cached, we can mark DONE immediately
    if os.path.exists(cached):
        write_job(job_id, {
            "job_id": job_id,
            "status": "DONE",
            "created_at": time.time(),
            "cache_key": key,
            "result": "/download/" + job_id,
            "message": "Served from cache"
        })
        return {"job_id": job_id, "status": "DONE"}

    # Otherwise create PENDING job and start thread
    write_job(job_id, {
        "job_id": job_id,
        "status": "PENDING",
        "created_at": time.time(),
        "cache_key": key,
        "message": "Queued"
    })

    t = threading.Thread(target=_run_generate, args=(job_id, req, key), daemon=True)
    t.start()

    return {"job_id": job_id, "status": "PENDING"}

@app.get("/job/{job_id}")
def job_status(job_id: str):
    data = read_job(job_id)
    return data

@app.get("/download/{job_id}")
def download(job_id: str):
    data = read_job(job_id)
    if data.get("status") != "DONE":
        raise HTTPException(status_code=409, detail="Job not completed")

    key = data.get("cache_key")
    if not key:
        raise HTTPException(status_code=500, detail="Missing cache key")

    path = cache_png_path(key)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Cached file not found")

    # nice filename
    return FileResponse(path, media_type="image/png", filename=f"{job_id}.png")
