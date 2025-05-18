import os
from dotenv import load_dotenv
load_dotenv()
import python_bithumb
from python_bithumb.private_api import Bithumb
import time
import threading

# bithumb_client는 스레드들이 공유하므로, 전역 또는 main에서 한 번만 생성합니다.
# 여기서는 main 함수 내에서 생성하는 것으로 유지하겠습니다.

def place_sell_order_and_wait(bithumb_api, ticker, price, volume):
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] Attempting to place sell order: {ticker}, Price: {price}, Volume: {volume}")
    response = bithumb_api.sell_limit_order(ticker, price, volume)
    position = None
    if response and response.get('uuid'):
        position = "sell"
        print(f"[{thread_name}] Sell order placed successfully. UUID: {response['uuid']}. Current position: {position}")
        # print(f"[{thread_name}] Response details:", response) # 상세 로그 필요시 주석 해제
        order_uuid = response['uuid']
        order = bithumb_api.get_order(order_uuid)
        # print(f"[{thread_name}] Initial order details:", order)
        state = order['state']
        while state != 'done':
            # print(f"[{thread_name}] Current order state: {state} for {order_uuid}, polling again in 1 second...") # 상세 폴링 로그 필요시 주석 해제
            time.sleep(1)
            order = bithumb_api.get_order(order_uuid)
            state = order['state']
        print(f"[{thread_name}] Order {order_uuid} (Sell) completed with state: {state}. Details: {order}")
        return order, position
    else:
        print(f"[{thread_name}] Sell order placement failed for {ticker}. Response: {response}")
        return None, position

def place_buy_order_and_wait(bithumb_api, ticker, price, volume):
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] Attempting to place buy order: {ticker}, Price: {price}, Volume: {volume}")
    response = bithumb_api.buy_limit_order(ticker, price, volume)
    position = None
    if response and response.get('uuid'):
        position = "buy"
        print(f"[{thread_name}] Buy order placed successfully. UUID: {response['uuid']}. Current position: {position}")
        # print(f"[{thread_name}] Response details:", response)
        order_uuid = response['uuid']
        order = bithumb_api.get_order(order_uuid)
        # print(f"[{thread_name}] Initial order details:", order)
        state = order['state']

        poll_count = 0
        max_polls = 30

        while state != 'done':
            if poll_count >= max_polls:
                print(f"[{thread_name}] Buy order {order_uuid} for {ticker} did not complete within {max_polls} polls. Last known state: {state}. Details: {order}")
                # Check if any part of the order was filled by looking at executed_volume
                if order and order.get('executed_volume') and float(order.get('executed_volume', "0")) > 0:
                    print(f"[{thread_name}] Buy order {order_uuid} was partially or fully filled (executed: {order.get('executed_volume')}). Setting position to 'buy' to attempt sell.")
                    position = "buy" # Some filled, prepare to sell
                else:
                    print(f"[{thread_name}] Buy order {order_uuid} was not filled (executed: {order.get('executed_volume', 'N/A')}). Setting position to 'sell' to re-attempt buy.")
                    position = "sell" # Not filled, re-attempt buy by setting position to 'sell'
                return order, position

            # print(f"[{thread_name}] Current order state: {state} for {order_uuid}, polling again in 1 second...")
            time.sleep(1)
            order = bithumb_api.get_order(order_uuid)
            state = order['state']
            poll_count += 1
        print(f"[{thread_name}] Order {order_uuid} (Buy) completed with state: {state}. Details: {order}")
        return order, position
    else:
        print(f"[{thread_name}] Buy order placement failed for {ticker}. Response: {response}")
        return None, position

def trade_continuously(bithumb_api_client, ticker, trade_amount, action_delay_seconds=1):
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] Starting continuous trading for {ticker}. Action delay: {action_delay_seconds}s")
    
    current_position = None  # None: 초기 상태, "buy": 매수 포지션, "sell": 매도 포지션
    last_buy_price = None

    while True:
        try:
            if current_position == "buy": # 매도 시도
                print(f"[{thread_name}] Current position for {ticker} is 'buy'. Attempting to sell.")
                orderbook = python_bithumb.get_orderbook(ticker)
                if not orderbook or not orderbook.get('orderbook_units'):
                    print(f"[{thread_name}] Failed to fetch orderbook for {ticker} to sell. Retrying after delay...")
                    time.sleep(action_delay_seconds)
                    continue
                
                price_for_sell = orderbook['orderbook_units'][0]['ask_price']
                print(f"[{thread_name}] Current ask price for {ticker}: {price_for_sell}")

                proceed_with_sell = False
                if last_buy_price is not None:
                    if float(price_for_sell) >= last_buy_price:
                        print(f"[{thread_name}] Sell condition met for {ticker}: Sell price {price_for_sell} >= Last buy price {last_buy_price}")
                        proceed_with_sell = True
                    else:
                        print(f"[{thread_name}] Sell condition NOT met for {ticker}: Sell price {price_for_sell} < Last buy price {last_buy_price}. Holding 'buy' position.")
                else:
                    # 이전에 매수한 기록이 없는데 포지션이 'buy'인 경우는 논리적으로 발생하기 어려우나, 방어적으로 매도 시도
                    print(f"[{thread_name}] Warning: Position is 'buy' but no last_buy_price for {ticker}. Attempting sell anyway.")
                    proceed_with_sell = True 

                if proceed_with_sell:
                    final_order, new_position = place_sell_order_and_wait(bithumb_api_client, ticker, price_for_sell, trade_amount)
                    if final_order and new_position == "sell":
                        current_position = "sell"
                        # last_buy_price는 매도 성공 시 초기화하지 않음. 다음 매수때까지 유지.
                        print(f"[{thread_name}] Sell successful for {ticker}. New position: {current_position}")
                    else:
                        print(f"[{thread_name}] Sell attempt for {ticker} failed or did not complete as expected. Retrying after delay...")
                        # current_position은 "buy"로 유지하고 재시도
                # else: 매도 조건 안맞으면 current_position "buy" 유지

            elif current_position == "sell" or current_position is None: # 매수 시도
                action_type = "Initial Buy" if current_position is None else "Buy (after sell)"
                print(f"[{thread_name}] Current position for {ticker} is '{current_position}'. Attempting {action_type}.")
                orderbook = python_bithumb.get_orderbook(ticker)
                if not orderbook or not orderbook.get('orderbook_units'):
                    print(f"[{thread_name}] Failed to fetch orderbook for {ticker} to buy. Retrying after delay...")
                    time.sleep(action_delay_seconds)
                    continue
                
                price_for_buy = orderbook['orderbook_units'][0]['bid_price']
                print(f"[{thread_name}] Current bid price for {ticker} (maker): {price_for_buy}")

                final_order, new_position = place_buy_order_and_wait(bithumb_api_client, ticker, price_for_buy, trade_amount)
                if final_order and new_position == "buy":
                    current_position = "buy"
                    try:
                        avg_price_str = final_order.get('avg_price')
                        order_price_str = final_order.get('price') # 지정가 주문시 입력한 가격
                        if avg_price_str and avg_price_str != "0":
                            last_buy_price = float(avg_price_str)
                            print(f"[{thread_name}] Stored last buy price for {ticker}: {last_buy_price} (from avg_price)")
                        elif order_price_str : # avg_price가 없을 경우 주문 가격을 사용
                            last_buy_price = float(order_price_str)
                            print(f"[{thread_name}] Stored last buy price for {ticker}: {last_buy_price} (from order price as fallback)")    
                        else:
                             print(f"[{thread_name}] Warning: Could not determine executed price for {ticker} from order details to store last_buy_price.")
                    except (ValueError, TypeError, KeyError) as e:
                        print(f"[{thread_name}] Error parsing price from buy order for {ticker}: {e}")
                    print(f"[{thread_name}] Buy successful for {ticker}. New position: {current_position}, Last buy price: {last_buy_price}")
                else:
                    print(f"[{thread_name}] Buy attempt for {ticker} failed or did not complete as expected. Retrying after delay...")
                    # current_position은 이전 상태("sell" or None) 유지하고 재시도
            else:
                # 논리적으로 도달해서는 안되는 상태
                print(f"[{thread_name}] Unexpected position '{current_position}' for {ticker}. Resetting. Retrying after delay...")
                current_position = None # 안전하게 초기화

            print(f"[{thread_name}] End of action for {ticker}. Current position: {current_position}, Last buy price: {last_buy_price}. Waiting for {action_delay_seconds}s...")
            time.sleep(action_delay_seconds)

        except Exception as e:
            print(f"[{thread_name}] An error occurred in trading loop for {ticker}: {e}. Retrying after delay...")
            time.sleep(action_delay_seconds)

def main():
    bithumb_api_client = Bithumb(os.getenv("BITHUMB_ACCESS_KEY"), os.getenv("BITHUMB_SECRET_KEY"))
    
    # .env 파일에서 거래 관련 설정값 로드
    usdt_trade_amount = float(os.getenv("USDT_TRADE_AMOUNT", "10")) # 기본값 10 USDT
    xrp_trade_amount = float(os.getenv("XRP_TRADE_AMOUNT", "10"))   # 기본값 10 XRP
    # 각 스레드 내의 개별 매수/매도 액션 후 대기 시간
    # API 호출 빈도 및 시장 상황에 맞춰 조절 필요
    action_delay_seconds = int(os.getenv("ACTION_DELAY_SECONDS", "1")) # 기본값 1초

    print("Starting continuous multi-threaded trading bot...")
    print(f"USDT Trade Amount: {usdt_trade_amount}, XRP Trade Amount: {xrp_trade_amount}, Action Delay: {action_delay_seconds}s")

    usdt_thread = threading.Thread(target=trade_continuously, 
                                   args=(bithumb_api_client, "KRW-USDT", usdt_trade_amount, action_delay_seconds),
                                   name="USDT-Trader")
    
    xrp_thread = threading.Thread(target=trade_continuously, 
                                  args=(bithumb_api_client, "KRW-XRP", xrp_trade_amount, action_delay_seconds),
                                  name="XRP-Trader")

    usdt_thread.start()
    time.sleep(1) # 두번째 스레드 시작 전 약간의 딜레이 (선택적)
    xrp_thread.start()

    try:
        usdt_thread.join() # 주 스레드는 자식 스레드가 끝날 때까지 (Ctrl+C 입력 시까지) 대기
        xrp_thread.join()
    except KeyboardInterrupt:
        print("\nBot stopping due to KeyboardInterrupt...")
        # 스레드가 정상적으로 종료될 시간을 줄 수도 있지만, 데몬 스레드가 아니므로 
        # 주 프로그램 종료 시 강제 종료될 수 있음.
        # 실제 서비스에서는 스레드에 종료 신호를 보내고 join하는 방식이 더 안전합니다.

    print("All trading threads have been signaled to stop or script interrupted.")

if __name__ == "__main__":
    main()

    