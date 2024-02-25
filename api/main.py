from typing import List, Optional
from fastapi import FastAPI, HTTPException, APIRouter
from fastapi.openapi.docs import get_swagger_ui_html
from pydantic import BaseModel, Field
from pymongo import MongoClient
from datetime import datetime

app = FastAPI(docs_url=None, redoc_url=None)  # Disable default docs

client = MongoClient("mongodb://mongo:27017/")  # Adjust if necessary
db = client.blockchainDB
router = APIRouter()

class Transaction(BaseModel):
    hash: str = Field(..., example="293cb3f47fc9da3cdd6b3cbf596dd81409d708374384bc8e1f9c7dc08303f855")
    blockNumber: int = Field(..., example=17043344)
    timestamp: int = Field(..., example=1708815974854)
    confirmations: int = Field(..., example=47)
    from_: str = Field(..., alias="from", example="NQ81 C01N BASE 0000 0000 0000 0000 0000 0000")
    fromType: int = Field(..., example=0)
    to: str = Field(..., example="NQ57 M1NT JRQA FGD2 HX1P FN2G 611P JNAE K7HN")
    toType: int = Field(..., example=0)
    value: int = Field(..., example=5554230)
    fee: int = Field(..., example=0)
    senderData: str = Field(..., example="")
    recipientData: str = Field(..., example="")
    flags: int = Field(..., example=0)
    validityStartHeight: int = Field(..., example=17043344)
    proof: str = Field(..., example="")
    networkId: int = Field(..., example=5)
    executionResult: bool = Field(..., example=True)

    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                "hash": "293cb3f47fc9da3cdd6b3cbf596dd81409d708374384bc8e1f9c7dc08303f855",
                "blockNumber": 17043344,
                "timestamp": 1708815974854,
                "confirmations": 47,
                "from": "NQ81 C01N BASE 0000 0000 0000 0000 0000 0000",
                "fromType": 0,
                "to": "NQ57 M1NT JRQA FGD2 HX1P FN2G 611P JNAE K7HN",
                "toType": 0,
                "value": 5554230,
                "fee": 0,
                "senderData": "",
                "recipientData": "",
                "flags": 0,
                "validityStartHeight": 17043344,
                "proof": "",
                "networkId": 5,
                "executionResult": True
            }
        }

class CountResponse(BaseModel):
    count: int

@router.get("/transactions/{block_number}", response_model=List[Transaction])
def read_transactions(block_number: int):
    transactions = list(db.transactions.find({"blockNumber": block_number}, {'_id': False}))
    if transactions:
        return transactions
    raise HTTPException(status_code=404, detail="Transactions not found")

@router.get("/transactions/hash/{tx_hash}", response_model=Transaction)
def read_transaction_by_hash(tx_hash: str):
    transaction = db.transactions.find_one({"hash": tx_hash}, {'_id': False})
    if transaction:
        return transaction
    raise HTTPException(status_code=404, detail="Transaction not found")

@router.get("/transactions/address/{address}", response_model=List[Transaction])
def read_transactions_by_address(address: str, limit: int = 50):
    transactions = list(db.transactions.find({"$or": [{"from": address}, {"to": address}]}, {'_id': False}).limit(limit))
    if transactions:
        return transactions
    raise HTTPException(status_code=404, detail="Transactions not found")

@router.get("/transactions/count/all", response_model=CountResponse)
def get_transactions_count():
    count = db.transactions.count_documents({})
    return CountResponse(count=count)

@router.get("/transactions/date", response_model=List[Transaction])
def read_transactions_by_date(start_date: str, end_date: str, limit: int = 50):
    start_timestamp = datetime.strptime(start_date, "%Y-%m-%d").timestamp()
    end_timestamp = datetime.strptime(end_date, "%Y-%m-%d").timestamp()
    transactions = list(db.transactions.find({
        "timestamp": {"$gte": start_timestamp, "$lte": end_timestamp}
    }, {'_id': False}).limit(limit))
    if transactions:
        return transactions
    raise HTTPException(status_code=404, detail="Transactions not found")

app.include_router(router, prefix="/api/v1")

@app.get("/docs", include_in_schema=False)
def custom_swagger_ui_html():
    return get_swagger_ui_html(openapi_url="/openapi.json", title="Custom Swagger UI")

# Add more endpoints as necessary for different query capabilities

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
