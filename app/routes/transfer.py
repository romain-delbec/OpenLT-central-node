from fastapi import APIRouter, WebSocket, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pandas as pd
import os
from app.config import DATA_DIR

router = APIRouter()

CENTRAL_INDEX = os.path.join(DATA_DIR, 'index', 'central_index.csv')

if not os.path.exists(CENTRAL_INDEX):
    df = pd.DataFrame(columns=["server_address", "portfolio_id", "navdate"])
    df.to_csv(CENTRAL_INDEX, index=False)

class UploadInfo(BaseModel):
    server_address: str
    portfolio_id: str
    navdate: str

@router.post("/upload/")
async def receive_upload(info: UploadInfo):
    df = pd.read_csv(CENTRAL_INDEX)
    
    new_entry = pd.DataFrame([{
        "server_address": info.server_address,
        "portfolio_id": info.portfolio_id,
        "navdate": info.navdate
    }])

    df = pd.concat([df, new_entry]).drop_duplicates()
    df.to_csv(CENTRAL_INDEX, index=False)
    
    return JSONResponse(content={"status": "added"}, status_code=200)

class LookupRequest(BaseModel):
    portfolio_id: str
    navdate: str

@router.post("/api/lookup")
async def lookup_endpoint(request: LookupRequest):
    try:
        df = pd.read_csv(CENTRAL_INDEX)
        df.columns = df.columns.str.strip()  # Clean column headers

        match = df[
            (df["portfolio_id"] == request.portfolio_id) &
            (df["navdate"] == request.navdate)
        ]

        if not match.empty:
            address = match.iloc[0]["server_address"]
            return {"server_address": address}
        else:
            return {"server_address": "Not found"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
