from typing import Optional, List

from pydantic import BaseModel


class NFTQuery(BaseModel):
    token_id: Optional[str] = None


class WalletQuery(BaseModel):
    address: Optional[str] = None

class NFTSQuery(BaseModel):
    nfts: Optional[str] = None

class TokensPoolQuery(BaseModel):
    token0: Optional[str] = None
    token1: Optional[str] = None


class PoolQuery(BaseModel):
    address: Optional[str] = None


class FeeQuery(BaseModel):
    pool_address: Optional[str] = None
    lower_price: Optional[float] = None
    upper_price: Optional[float] = None
