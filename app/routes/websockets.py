import os
import shutil
import pandas as pd 
from pydantic import BaseModel
from pathlib import Path
from fastapi import APIRouter, Request, UploadFile, File, WebSocket, WebSocketDisconnect, HTTPException, Form
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from app.config import DATA_DIR

websockets = APIRouter()