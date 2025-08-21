from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import asyncio
import aiohttp
from pydantic import BaseModel

app = FastAPI(title="Financial Data API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class FinancialData(BaseModel):
    symbol: str
    name: str
    price: float
    change: Optional[float] = None
    change_percent: Optional[float] = None
    currency: str
    last_updated: str
    category: str

class APIResponse(BaseModel):
    data: List[FinancialData]
    total_count: int
    timestamp: str

# Listy symboli do pobierania
STOCK_SYMBOLS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK-B', 'JNJ', 'V',
    'WMT', 'PG', 'UNH', 'HD', 'MA', 'DIS', 'PYPL', 'BAC', 'ADBE', 'NFLX',
    'CRM', 'XOM', 'VZ', 'KO', 'PFE', 'INTC', 'CSCO', 'PEP', 'ABT', 'TMO',
    'COST', 'CVX', 'AVGO', 'ACN', 'MRK', 'LLY', 'NKE', 'DHR', 'NEE', 'ORCL'
]

CRYPTO_SYMBOLS = [
    'BTC-USD', 'ETH-USD', 'BNB-USD', 'XRP-USD', 'ADA-USD', 
    'SOL-USD', 'DOGE-USD', 'DOT-USD', 'MATIC-USD', 'LTC-USD'
]

COMMODITY_SYMBOLS = [
    'GC=F',  # Gold
    'SI=F',  # Silver
    'CL=F',  # Crude Oil
    'NG=F',  # Natural Gas
]

def get_stock_data(symbols: List[str]) -> List[FinancialData]:
    """Pobiera dane o akcjach z Yahoo Finance"""
    results = []
    
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            hist = ticker.history(period="2d")
            
            if len(hist) >= 2:
                current_price = hist['Close'].iloc[-1]
                previous_price = hist['Close'].iloc[-2]
                change = current_price - previous_price
                change_percent = (change / previous_price) * 100
            else:
                current_price = info.get('currentPrice', 0)
                change = info.get('regularMarketChange', 0)
                change_percent = info.get('regularMarketChangePercent', 0)
            
            data = FinancialData(
                symbol=symbol,
                name=info.get('longName', symbol),
                price=round(float(current_price), 2),
                change=round(float(change), 2) if change else None,
                change_percent=round(float(change_percent), 2) if change_percent else None,
                currency=info.get('currency', 'USD'),
                last_updated=datetime.now().isoformat(),
                category='stock'
            )
            results.append(data)
            
        except Exception as e:
            print(f"Błąd pobierania danych dla {symbol}: {e}")
            continue
    
    return results

def get_crypto_data(symbols: List[str]) -> List[FinancialData]:
    """Pobiera dane o kryptowalutach z Yahoo Finance"""
    results = []
    
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            hist = ticker.history(period="2d")
            
            if len(hist) >= 2:
                current_price = hist['Close'].iloc[-1]
                previous_price = hist['Close'].iloc[-2]
                change = current_price - previous_price
                change_percent = (change / previous_price) * 100
            else:
                current_price = info.get('regularMarketPrice', 0)
                change = info.get('regularMarketChange', 0)
                change_percent = info.get('regularMarketChangePercent', 0)
            
            # Nazwa kryptowaluty bez -USD
            crypto_name = symbol.replace('-USD', '').upper()
            
            data = FinancialData(
                symbol=symbol,
                name=f"{crypto_name} (Cryptocurrency)",
                price=round(float(current_price), 2),
                change=round(float(change), 2) if change else None,
                change_percent=round(float(change_percent), 2) if change_percent else None,
                currency='USD',
                last_updated=datetime.now().isoformat(),
                category='cryptocurrency'
            )
            results.append(data)
            
        except Exception as e:
            print(f"Błąd pobierania danych dla {symbol}: {e}")
            continue
    
    return results

def get_commodity_data(symbols: List[str]) -> List[FinancialData]:
    """Pobiera dane o surowcach z Yahoo Finance"""
    results = []
    commodity_names = {
        'GC=F': 'Gold',
        'SI=F': 'Silver',
        'CL=F': 'Crude Oil',
        'NG=F': 'Natural Gas'
    }
    
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="2d")
            
            if len(hist) >= 2:
                current_price = hist['Close'].iloc[-1]
                previous_price = hist['Close'].iloc[-2]
                change = current_price - previous_price
                change_percent = (change / previous_price) * 100
                
                data = FinancialData(
                    symbol=symbol,
                    name=commodity_names.get(symbol, symbol),
                    price=round(float(current_price), 2),
                    change=round(float(change), 2),
                    change_percent=round(float(change_percent), 2),
                    currency='USD',
                    last_updated=datetime.now().isoformat(),
                    category='commodity'
                )
                results.append(data)
            
        except Exception as e:
            print(f"Błąd pobierania danych dla {symbol}: {e}")
            continue
    
    return results

@app.get("/", response_model=dict)
async def root():
    """Główny endpoint z informacjami o API"""
    return {
        "message": "Financial Data API",
        "version": "1.0.0",
        "endpoints": {
            "/all": "Wszystkie dane finansowe",
            "/stocks": "Tylko akcje",
            "/crypto": "Tylko kryptowaluty",
            "/commodities": "Tylko surowce (złoto, srebro, etc.)",
            "/search/{symbol}": "Wyszukaj konkretny symbol"
        }
    }

@app.get("/all", response_model=APIResponse)
async def get_all_data():
    """Pobiera wszystkie dane finansowe - akcje, krypto i surowce"""
    try:
        all_data = []
        
        # Pobierz dane akcji
        stock_data = get_stock_data(STOCK_SYMBOLS[:25])  # Pierwszych 25 akcji
        all_data.extend(stock_data)
        
        # Pobierz dane krypto
        crypto_data = get_crypto_data(CRYPTO_SYMBOLS)
        all_data.extend(crypto_data)
        
        # Pobierz dane surowców
        commodity_data = get_commodity_data(COMMODITY_SYMBOLS)
        all_data.extend(commodity_data)
        
        return APIResponse(
            data=all_data,
            total_count=len(all_data),
            timestamp=datetime.now().isoformat()
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd pobierania danych: {str(e)}")

@app.get("/stocks", response_model=APIResponse)
async def get_stocks():
    """Pobiera dane tylko o akcjach"""
    try:
        stock_data = get_stock_data(STOCK_SYMBOLS)
        
        return APIResponse(
            data=stock_data,
            total_count=len(stock_data),
            timestamp=datetime.now().isoformat()
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd pobierania danych akcji: {str(e)}")

@app.get("/crypto", response_model=APIResponse)
async def get_crypto():
    """Pobiera dane tylko o kryptowalutach"""
    try:
        crypto_data = get_crypto_data(CRYPTO_SYMBOLS)
        
        return APIResponse(
            data=crypto_data,
            total_count=len(crypto_data),
            timestamp=datetime.now().isoformat()
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd pobierania danych krypto: {str(e)}")

@app.get("/commodities", response_model=APIResponse)
async def get_commodities():
    """Pobiera dane o surowcach (złoto, srebro, ropa, etc.)"""
    try:
        commodity_data = get_commodity_data(COMMODITY_SYMBOLS)
        
        return APIResponse(
            data=commodity_data,
            total_count=len(commodity_data),
            timestamp=datetime.now().isoformat()
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd pobierania danych surowców: {str(e)}")

@app.get("/search/{symbol}")
async def search_symbol(symbol: str):
    """Wyszukuje dane dla konkretnego symbolu"""
    try:
        # Spróbuj znaleźć symbol w różnych kategoriach
        all_symbols = STOCK_SYMBOLS + CRYPTO_SYMBOLS + COMMODITY_SYMBOLS
        
        if symbol.upper() in [s.upper() for s in all_symbols]:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            hist = ticker.history(period="2d")
            
            if len(hist) >= 2:
                current_price = hist['Close'].iloc[-1]
                previous_price = hist['Close'].iloc[-2]
                change = current_price - previous_price
                change_percent = (change / previous_price) * 100
            else:
                current_price = info.get('regularMarketPrice', 0)
                change = info.get('regularMarketChange', 0)
                change_percent = info.get('regularMarketChangePercent', 0)
            
            # Określ kategorię
            category = 'stock'
            if symbol in CRYPTO_SYMBOLS:
                category = 'cryptocurrency'
            elif symbol in COMMODITY_SYMBOLS:
                category = 'commodity'
            
            data = FinancialData(
                symbol=symbol,
                name=info.get('longName', symbol),
                price=round(float(current_price), 2),
                change=round(float(change), 2) if change else None,
                change_percent=round(float(change_percent), 2) if change_percent else None,
                currency=info.get('currency', 'USD'),
                last_updated=datetime.now().isoformat(),
                category=category
            )
            
            return {"data": data}
        
        else:
            raise HTTPException(status_code=404, detail=f"Symbol {symbol} nie został znaleziony")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Błąd wyszukiwania symbolu: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)