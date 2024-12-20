# python-bithumb
`python-bithumb`는 Bithumb의 Public/Private API를 Python에서 간편하게 사용할 수 있는 래퍼 라이브러리입니다.  
Bithumb의 시세 조회, 캔들(OHLCV) 조회, 주문(지정가/시장가), 잔고 조회, 주문 취소, 개별 주문 조회, 주문 가능 정보 등을 다룰 수 있습니다.

## python-bithumb 가이드
`python-bithumb`의 소스 코드 기반의 지식(Knowledge)을 가진 GPTs에게 질문하시면 더 자세한 가이드를 얻으실 수 있습니다.
python-bithumb GPTs 링크: [https://chatgpt.com/g/g-6764a2fc67988191a4382c8511d509d0-python-bithumb-gaideu](https://chatgpt.com/g/g-6764a2fc67988191a4382c8511d509d0-python-bithumb-gaideu)

## 주요 특징
- **Public API**:  
  - OHLCV(일봉, 분봉, 주봉, 월봉) 조회
  - 현재가 정보 조회
  - 호가 정보 조회
  - 마켓 코드 조회, 최근 체결 내역 조회, 경보 종목 조회

- **Private API** (Access Key & Secret Key 필요):  
  - 전체 계좌(잔고) 조회
  - 지정가/시장가 매수·매도 주문
  - 주문 취소, 개별 주문 조회, 주문 가능 정보 조회
  - 주문 리스트 조회 등 추가 확장 기능

## 설치 (Installation)

```bash
pip install python-bithumb
```
또는 소스 코드 직접 다운로드 후 설치:

```bash
python setup.py install
```

## 시작하기 (Getting Started)
### Public API 예시
```python
import python_bithumb

# 특정 마켓의 일봉 데이터 조회 (최근 5일)
df = python_bithumb.get_ohlcv("KRW-BTC", interval="day", count=5)
print(df)

# 특정 마켓의 1분봉 데이터 조회 (최근 10개)
df_min = python_bithumb.get_ohlcv("KRW-BTC", interval="minute1", count=10)
print(df_min)

# 현재가 조회 (단일 티커)
price = python_bithumb.get_current_price("KRW-BTC")
print(price)

# 현재가 조회 (다중 티커)
prices = python_bithumb.get_current_price(["KRW-BTC", "BTC-ETH"])
print(prices)  # {"KRW-BTC": float, "BTC-ETH": float}

# 호가 정보 조회 (단일 티커)
orderbook = python_bithumb.get_orderbook("KRW-BTC")
print(orderbook)
```

### Private API (개인 인증 정보 필요)
Private API를 사용하려면 Bithumb에서 발급받은 Access Key와 Secret Key가 필요합니다.
이 키를 이용해 Bithumb 객체를 생성한 후 잔고 조회나 주문 기능을 사용할 수 있습니다.
```python
import python_bithumb

access_key = "access_key"
secret_key = "secret_key"

bithumb = python_bithumb.Bithumb(access_key, secret_key)

# 전체 계좌 조회
balances = bithumb.get_balances()
print(balances)

# 특정 화폐의 잔고 조회
krw_balance = bithumb.get_balance("KRW")
print(krw_balance)

# 지정가 매수 주문 (예: KRW-BTC를 139,000,000원에 0.0001 BTC 매수)
order_info = bithumb.buy_limit_order("KRW-BTC", 139000000, 0.0001)
print(order_info)

# 지정가 매도 주문 (예: KRW-BTC를 155,000,000원에 0.0001 BTC 매도)
order_info = bithumb.sell_limit_order("KRW-BTC", 155000000, 0.0001)
print(order_info)

# 시장가 매수 주문 (예: KRW-BTC를 10,000원어치 시장가 매수)
order_info = bithumb.buy_market_order("KRW-BTC", 10000)
print(order_info)

# 시장가 매도 주문 (예: KRW-BTC를 0.0001 BTC 전량 시장가 매도)
order_info = bithumb.sell_market_order("KRW-BTC", 0.0001)
print(order_info)

# 주문 가능 정보 조회 (주문 전 최소 거래금액, 수수료 등 확인)
chance_info = bithumb.get_order_chance("KRW-BTC")
print(chance_info)

# 개별 주문 조회 (UUID 필요)
order_detail = bithumb.get_order("주문_UUID")
print(order_detail)

# 주문 리스트 조회
orders = bithumb.get_orders(market="KRW-BTC", limit=5)
print(orders)

# 주문 취소 (UUID 필요)
cancel_result = bithumb.cancel_order("주문_UUID")
print(cancel_result)
```

## 함수 정리
### Public API 함수
- get_ohlcv(ticker, interval="day", count=200, period=0.1, to=None)
 - 특정 마켓의 캔들 데이터를 Pandas DataFrame으로 반환.
 - interval: 조회 간격. "day" (일봉, 기본값), "week" (주봉), "month" (월봉), "minute1", "minute3", "minute5", "minute10", "minute15", "minute30", "minute60", "minute240".
 - count: 조회할 캔들 개수 (최대 200개).
 - to: 마지막 캔들의 기준 시간 (ISO 8601 형식).
 - period: API 호출 간 간격 (초 단위).
- get_current_price(markets)
 - 현재가 조회 (단일/복수 종목 가능).
- get_orderbook(markets)
 - 호가 정보 조회.
그 외 get_market_all, get_trades_ticks, get_virtual_asset_warning 등을 통해 마켓 코드, 최근 체결, 경보 종목 정보도 조회 가능.

### Private API 함수 (Bithumb 클래스)
- get_balances()
전체 계좌(잔고) 정보 조회.

- buy_limit_order(ticker, price, volume)
지정가 매수 주문.

- sell_limit_order(ticker, price, volume)
지정가 매도 주문.

- buy_market_order(ticker, krw_amount)
시장가 매수 (금액 기준).

- sell_market_order(ticker, volume)
시장가 매도 (수량 기준).

- get_order_chance(market)
주문 가능 정보 조회 (수수료, 최소 거래금액, 지원 주문 방식 등).

- get_order(uuid), get_orders(...)
개별 주문 조회, 주문 리스트 조회.

- cancel_order(uuid)
주문 취소.

## 주의사항
- 수수료 및 최소 거래금액: 빗썸은 최소 거래금액(5,000원 이상) 조건 및 수수료가 있습니다. 지정가 주문 시, 최소 거래금액을 만족하도록 가격과 수량을 조정해야 합니다.
- 잔고 부족 에러(HTTP 400): 실제 보유한 BTC나 KRW보다 많은 수량/금액을 주문할 경우 에러가 발생할 수 있습니다. 주문 전 get_balance 등을 통해 충분한 잔고가 있는지 확인하십시오.
- 테스트 시 실제 거래 발생: 테스트를 위해 시장가 주문 등을 호출하면 실제 매매가 발생하며 수수료가 지출될 수 있습니다. 테스트용 소액, 별도 계정 사용을 권장합니다.
- API 사양 변경 시 대응 필요: Bithumb API 사양 변경 시 코드 수정이 필요할 수 있습니다.

## 라이선스
- 이 프로젝트는 Apache License 2.0 하에 배포됩니다.