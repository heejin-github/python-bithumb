import requests
import pandas as pd
import time

class BithumbAPIException(Exception):
    """Exception raised for Bithumb API errors.
    
    Attributes:
        status_code -- HTTP status code
        error_msg -- error message from the API
        response -- full response object
    """
    
    def __init__(self, status_code, error_msg, response):
        self.status_code = status_code
        self.error_msg = error_msg
        self.response = response
        super().__init__(f"Bithumb API Error (HTTP {status_code}): {error_msg}")

def _handle_response(response):
    """Process API response and handle errors appropriately.
    
    Args:
        response: requests Response object
        
    Returns:
        Parsed JSON response data
        
    Raises:
        BithumbAPIException: If API returns an error
    """
    if response.status_code != 200:
        error_msg = "Unknown error"
        try:
            error_data = response.json()
            if isinstance(error_data, dict) and 'error' in error_data:
                error_info = error_data['error']
                if isinstance(error_info, dict):
                    error_name = error_info.get('name', 'Unknown error code')
                    error_message = error_info.get('message', 'No error message provided')
                    error_msg = f"Error {error_name}: {error_message}"
                else:
                    error_msg = str(error_info)
        except:
            error_msg = response.text or "Could not parse error response"
        
        raise BithumbAPIException(response.status_code, error_msg, response)
    
    return response.json()

def get_ohlcv(ticker: str, interval: str = "day", count: int = 200, period: float = 0.1, to: str = None):
    base_url = "https://api.bithumb.com/v1"

    if interval == "day":
        endpoint = "candles/days"
    elif interval == "week":
        endpoint = "candles/weeks"
    elif interval == "month":
        endpoint = "candles/months"
    elif interval.startswith("minute"):
        unit_str = interval.replace("minute", "")
        try:
            unit = int(unit_str)
        except ValueError:
            unit = 1
        if unit not in [1,3,5,10,15,30,60,240]:
            raise ValueError("Invalid interval unit for minute candles. Choose from [1,3,5,10,15,30,60,240].")

        endpoint = f"candles/minutes/{unit}"
    else:
        endpoint = "candles/days"

    max_count = 200
    all_data = []
    remaining = count
    current_to = to

    while remaining > 0:
        fetch_count = min(remaining, max_count)
        params = {"market": ticker, "count": fetch_count}
        if current_to:
            params["to"] = current_to

        resp = requests.get(f"{base_url}/{endpoint}", params=params)
        data = _handle_response(resp)

        if not isinstance(data, list) or len(data) == 0:
            break

        all_data.extend(data)
        remaining -= fetch_count

        last_candle = data[-1]
        last_time_str = last_candle["candle_date_time_kst"]
        current_to = last_time_str

        if remaining > 0:
            time.sleep(period)

    if len(all_data) == 0:
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    df['candle_date_time_kst'] = pd.to_datetime(df['candle_date_time_kst'])
    df.set_index('candle_date_time_kst', inplace=True)
    df.sort_index(inplace=True)
    df.rename(columns={
        "opening_price": "open",
        "high_price": "high",
        "low_price": "low",
        "trade_price": "close",
        "candle_acc_trade_volume": "volume",
        "candle_acc_trade_price": "value"
    }, inplace=True)
    return df

def get_current_price(markets):
    base_url = "https://api.bithumb.com/v1"
    if isinstance(markets, list):
        market_str = ",".join(markets)
    else:
        market_str = markets

    params = {"markets": market_str}
    resp = requests.get(f"{base_url}/ticker", params=params)
    data = _handle_response(resp)

    if isinstance(data, list):
        ticker_data = data
    elif isinstance(data, dict):
        ticker_data = data.get("data", [])
    else:
        ticker_data = []

    if len(ticker_data) == 0:
        return None

    if len(ticker_data) == 1:
        return float(ticker_data[0]["trade_price"])
    else:
        result = {}
        for item in ticker_data:
            market = item["market"]
            result[market] = float(item["trade_price"])
        return result

def get_orderbook(markets):
    base_url = "https://api.bithumb.com/v1"
    if isinstance(markets, list):
        market_str = ",".join(markets)
    else:
        market_str = markets

    params = {"markets": market_str}
    resp = requests.get(f"{base_url}/orderbook", params=params)
    data = _handle_response(resp)

    if isinstance(data, list):
        orderbook_data = data
    elif isinstance(data, dict):
        orderbook_data = data.get("data", [])
    else:
        orderbook_data = []

    if len(orderbook_data) == 0:
        return None

    def parse_orderbook(item):
        return {
            "market": item["market"],
            "timestamp": item["timestamp"],
            "total_ask_size": item["total_ask_size"],
            "total_bid_size": item["total_bid_size"],
            "orderbook_units": item["orderbook_units"]
        }

    if len(orderbook_data) == 1:
        return parse_orderbook(orderbook_data[0])
    else:
        result = {}
        for item in orderbook_data:
            m = item["market"]
            result[m] = parse_orderbook(item)
        return result
    
def get_market_all():
    """
    빗썸에서 거래 가능한 마켓 정보 조회
    
    Response 예시:
    [
        {
            "market": "KRW-BTC",
            "korean_name": "비트코인",
            "english_name": "Bitcoin"
        },
        ...
    ]
    """
    base_url = "https://api.bithumb.com/v1"
    resp = requests.get(f"{base_url}/market/all")
    return _handle_response(resp)

def get_trades_ticks(market: str, to: str = None, count: int = 1, cursor: str = None, daysAgo: int = None):
    """
    최근 체결 내역 조회
    
    응답 예시:
    [
      {
        "market": "KRW-BTC",
        "trade_date_utc": "2018-04-18",
        "trade_time_utc": "10:19:58",
        "timestamp": 1524046798000,
        "trade_price": 8616000,
        "trade_volume": 0.03060688,
        "prev_closing_price": 8450000,
        "chane_price": 166000,
        "ask_bid": "ASK"
      }
    ]
    """
    base_url = "https://api.bithumb.com/v1"
    params = {"market": market, "count": count}
    if to:
        params["to"] = to
    if cursor:
        params["cursor"] = cursor
    if daysAgo is not None:
        params["daysAgo"] = daysAgo

    resp = requests.get(f"{base_url}/trades/ticks", params=params)
    return _handle_response(resp)

def get_virtual_asset_warning():
    """
    경보중인 마켓-코인 목록 조회
    
    응답 예시:
    [
      {
        "market": "KRW-BTC",
        "warning_type": "PRICE_SUDDEN_FLUCTUATION",
        "end_date": "2023-12-18 18:23:16"
      },
      ...
    ]
    """
    base_url = "https://api.bithumb.com/v1"
    resp = requests.get(f"{base_url}/market/virtual_asset_warning")
    return _handle_response(resp)