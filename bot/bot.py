import os
from dotenv import load_dotenv
load_dotenv()
import python_bithumb
from python_bithumb.private_api import Bithumb
import time
import threading
from datetime import datetime
import numpy as np
import requests
import json

def log_with_timestamp(message):
    print(f"{datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')} {message}")

# bithumb_client는 스레드들이 공유하므로, 전역 또는 main에서 한 번만 생성합니다.
# 여기서는 main 함수 내에서 생성하는 것으로 유지하겠습니다.

def place_sell_order_and_wait(bithumb_api, ticker, price, volume):
    thread_name = threading.current_thread().name
    log_with_timestamp(f"[{thread_name}] Attempting to place sell order: {ticker}, Price: {price}, Volume: {volume}")
    response = bithumb_api.sell_limit_order(ticker, price, volume)
    position = None
    if response and response.get('uuid'):
        position = "sell"
        log_with_timestamp(f"[{thread_name}] Sell order placed successfully. UUID: {response['uuid']}. Current position: {position}")
        order_uuid = response['uuid']
        order = bithumb_api.get_order(order_uuid)
        state = order['state']

        while state != 'done':
            # 긴급 매도 조건 확인
            if check_emergency_sell_conditions(ticker):
                log_with_timestamp(f"[{thread_name}] Emergency sell conditions detected while waiting for sell order. Canceling current order and executing market sell.")
                try:
                    # 현재 주문 취소
                    cancel_status = bithumb_api.cancel_order(order_uuid)
                    log_with_timestamp(f"[{thread_name}] Cancel order {order_uuid} attempt status: {cancel_status}")

                    # 시장가 매도 주문
                    market_sell_response = bithumb_api.sell_market_order(ticker, volume)
                    if market_sell_response and market_sell_response.get('uuid'):
                        market_order_uuid = market_sell_response['uuid']
                        market_order = bithumb_api.get_order(market_order_uuid)

                        # 거래 정보 로깅
                        executed_volume = float(market_order.get('executed_volume', 0))
                        # trades 배열에서 평균 가격 계산
                        trades = market_order.get('trades', [])
                        if trades:
                            total_funds = sum(float(trade['funds']) for trade in trades)
                            executed_price = total_funds / executed_volume if executed_volume > 0 else 0
                        else:
                            executed_price = 0

                        log_with_timestamp(f"\n=== Emergency Market Sell Details for {ticker} ===")
                        log_with_timestamp(f"Original Limit Order Price: {price:,.2f}")
                        log_with_timestamp(f"Market Sell Price: {executed_price:,.2f}")
                        log_with_timestamp(f"Volume: {executed_volume:,.8f}")

                        # 매수 가격이 저장되어 있다면 손실 금액도 계산
                        if hasattr(threading.current_thread(), 'last_buy_price') and threading.current_thread().last_buy_price:
                            loss_amount = (threading.current_thread().last_buy_price - executed_price) * executed_volume
                            log_with_timestamp(f"Loss Amount: {loss_amount:,.2f} KRW")

                            # 디스코드 알림 전송 (로그는 그대로 유지)
                            notification_title = f"⚠️ Emergency Market Sell Executed: {ticker}"
                            notification_message = (
                                f"**Emergency Market Sell Details**\n\n"
                                f"Original Limit Order Price: {price:,.2f}\n"
                                f"Market Sell Price: {executed_price:,.2f}\n"
                                f"Volume: {executed_volume:,.8f}\n"
                                f"Loss Amount: {loss_amount:,.2f} KRW"
                            )

                            # 큰 손실 발생 시 추가 알림
                            if loss_amount > 0:  # 손실 발생
                                notification_title = f"💔 Loss Alert: {ticker}"

                            send_discord_notification(notification_message, notification_title)

                        # 매도 성공 시에만 포지션 초기화
                        position = None
                        threading.current_thread().last_buy_price = None
                        threading.current_thread().last_buy_volume = None
                        return market_order, position
                    else:
                        log_with_timestamp(f"[{thread_name}] Market sell order placement failed. Maintaining buy position for retry.")
                        return None, "buy"  # 매도 실패 시 buy 포지션 유지
                except Exception as e:
                    log_with_timestamp(f"[{thread_name}] Error during emergency market sell: {e}")
                    # 에러 발생 시에도 buy 포지션 유지
                    return None, "buy"

            time.sleep(1)
            order = bithumb_api.get_order(order_uuid)
            state = order['state']

        log_with_timestamp(f"[{thread_name}] Order {order_uuid} (Sell) completed with state: {state}. Details: {order}")
        # 매도 주문 체결 후 cooldown time 적용
        cooldown_seconds = int(os.getenv("ORDER_COOLDOWN_SECONDS", "5"))  # 기본값 5초
        log_with_timestamp(f"[{thread_name}] Applying cooldown time of {cooldown_seconds} seconds after sell order execution.")
        time.sleep(cooldown_seconds)
        return order, position
    else:
        log_with_timestamp(f"[{thread_name}] Sell order placement failed for {ticker}. Response: {response}")
        return None, "buy"  # 매도 실패 시 buy 포지션 유지

def place_buy_order_and_wait(bithumb_api, ticker, price, volume):
    thread_name = threading.current_thread().name
    log_with_timestamp(f"[{thread_name}] Attempting to place buy order: {ticker}, Price: {price}, Volume: {volume}")
    response = bithumb_api.buy_limit_order(ticker, price, volume)
    position = None
    if response and response.get('uuid'):
        position = "buy"
        log_with_timestamp(f"[{thread_name}] Buy order placed successfully. UUID: {response['uuid']}. Current position: {position}")
        order_uuid = response['uuid']
        order = bithumb_api.get_order(order_uuid)
        state = order['state']
        poll_count = 0
        max_polls = int(os.getenv("MAX_POLLS", "30"))  # 기본값 30회
        last_executed_volume = 0.0
        while state != 'done':
            if poll_count >= max_polls:
                # 현재 체결된 수량 확인
                current_executed_volume = float(order.get('executed_volume', 0))
                if current_executed_volume > last_executed_volume:
                    # 새로운 체결이 발생한 경우, 폴링 카운트 리셋
                    log_with_timestamp(f"[{thread_name}] New execution detected for order {order_uuid}. Resetting poll count.")
                    poll_count = 0
                    last_executed_volume = current_executed_volume
                    continue
                log_with_timestamp(f"[{thread_name}] Buy order {order_uuid} for {ticker} (Original Price: {price}) did not complete within {max_polls} polls. Checking current market bid price...")
                current_orderbook = python_bithumb.get_orderbook(ticker)
                if current_orderbook and current_orderbook.get('orderbook_units') and len(current_orderbook['orderbook_units']) > 0:
                    current_bid_price_str = current_orderbook['orderbook_units'][0]['bid_price']
                    log_with_timestamp(f"[{thread_name}] Original buy price for {order_uuid}: {price}, Current market bid price for {ticker}: {current_bid_price_str}")
                    if float(current_bid_price_str) == float(price):
                        # 시장 가격이 주문 가격과 동일한 경우, 주문 유지
                        log_with_timestamp(f"[{thread_name}] Market bid price ({current_bid_price_str}) is same as order price ({price}). Resetting poll count for order {order_uuid}.")
                        poll_count = 0
                        order = bithumb_api.get_order(order_uuid)
                        state = order['state']
                        continue
                    else:
                        # 시장 가격이 변경된 경우, 남은 수량 취소 처리
                        remaining_volume = float(order.get('remaining_volume', 0))
                        if remaining_volume > 0:
                            # 취소 전에 현재까지의 체결 수량 저장
                            last_known_executed_volume = float(order.get('executed_volume', 0))

                            log_with_timestamp(f"[{thread_name}] Market bid price ({current_bid_price_str}) differs from order price ({price}). Attempting to cancel remaining volume ({remaining_volume}) for order {order_uuid}.")

                            # 취소 전에 한 번 더 주문 상태 확인
                            try:
                                final_check_order = bithumb_api.get_order(order_uuid)
                                if final_check_order['state'] == 'done':
                                    log_with_timestamp(f"[{thread_name}] Order {order_uuid} was completed before cancellation. Processing completed order.")
                                    order = final_check_order
                                    state = 'done'
                                    break

                                # 주문이 아직 진행 중인 경우에만 취소 시도
                                cancel_status = bithumb_api.cancel_order(order_uuid)
                                log_with_timestamp(f"[{thread_name}] Cancel order {order_uuid} attempt status: {cancel_status}")

                                if last_known_executed_volume > 0:
                                    # 부분 체결된 경우, 체결된 수량만큼 매도 시도
                                    log_with_timestamp(f"[{thread_name}] Selling executed volume ({last_known_executed_volume}) from partially filled order.")
                                    sell_order, _ = place_sell_order_and_wait(bithumb_api, ticker, current_bid_price_str, last_known_executed_volume)
                                    if sell_order:
                                        log_with_timestamp(f"[{thread_name}] Successfully sold {last_known_executed_volume} {ticker} from partially filled order.")
                                        position = "sell"
                                    else:
                                        log_with_timestamp(f"[{thread_name}] Failed to sell {last_known_executed_volume} {ticker} from partially filled order.")
                                else:
                                    # 체결된 수량이 없는 경우, 포지션 초기화
                                    log_with_timestamp(f"[{thread_name}] No executed volume for order {order_uuid}. Resetting position to None.")
                                    position = None
                                return order, position
                            except Exception as e:
                                if "order_not_found" in str(e):
                                    # 주문이 이미 체결된 경우
                                    log_with_timestamp(f"[{thread_name}] Order {order_uuid} was completed before cancellation. Processing completed order.")
                                    state = 'done'
                                    break
                                else:
                                    # 다른 에러의 경우
                                    log_with_timestamp(f"[{thread_name}] Error during order cancellation: {e}")
                                    raise
                        else:
                            log_with_timestamp(f"[{thread_name}] Order {order_uuid} is already fully executed.")
                            state = 'done'
                            continue
                else:
                    # 호가창 조회 실패 시 주문 취소 처리
                    log_with_timestamp(f"[{thread_name}] Failed to fetch current orderbook for {ticker} or orderbook empty. Proceeding to cancel order {order_uuid} as a fallback.")
                    try:
                        # 취소 전에 한 번 더 주문 상태 확인
                        final_check_order = bithumb_api.get_order(order_uuid)
                        if final_check_order['state'] == 'done':
                            log_with_timestamp(f"[{thread_name}] Order {order_uuid} was completed before cancellation. Processing completed order.")
                            order = final_check_order
                            state = 'done'
                            break

                        cancel_status = bithumb_api.cancel_order(order_uuid)
                        log_with_timestamp(f"[{thread_name}] Fallback cancel order {order_uuid} attempt status: {cancel_status}.")
                        if current_executed_volume > 0:
                            # 부분 체결된 경우, 체결된 수량만큼 매도 시도
                            log_with_timestamp(f"[{thread_name}] Selling executed volume ({current_executed_volume}) from partially filled order.")
                            sell_order, _ = place_sell_order_and_wait(bithumb_api, ticker, price, current_executed_volume)
                            if sell_order:
                                log_with_timestamp(f"[{thread_name}] Successfully sold {current_executed_volume} {ticker} from partially filled order.")
                                position = "sell"
                            else:
                                log_with_timestamp(f"[{thread_name}] Failed to sell {current_executed_volume} {ticker} from partially filled order.")
                        else:
                            # 체결된 수량이 없는 경우, 포지션 초기화
                            log_with_timestamp(f"[{thread_name}] No executed volume for order {order_uuid}. Resetting position to None.")
                            position = None
                        return order, position
                    except Exception as e:
                        if "order_not_found" in str(e):
                            # 주문이 이미 체결된 경우
                            log_with_timestamp(f"[{thread_name}] Order {order_uuid} was completed before cancellation. Processing completed order.")
                            state = 'done'
                            break
                        else:
                            # 다른 에러의 경우
                            log_with_timestamp(f"[{thread_name}] Error during fallback order cancellation: {e}")
                            raise
            time.sleep(1)
            order = bithumb_api.get_order(order_uuid)
            state = order['state']
            poll_count += 1
        # 주문 완료 시 체결 여부 확인
        if state == 'done' and float(order.get('executed_volume', 0)) > 0:
            log_with_timestamp(f"[{thread_name}] Order {order_uuid} (Buy) completed with state: {state}. Details: {order}")
            # 매수 주문 체결 후 cooldown time 적용
            cooldown_seconds = int(os.getenv("ORDER_COOLDOWN_SECONDS", "5"))  # 기본값 5초
            log_with_timestamp(f"[{thread_name}] Applying cooldown time of {cooldown_seconds} seconds after buy order execution.")
            time.sleep(cooldown_seconds)
            return order, position
        else:
            log_with_timestamp(f"[{thread_name}] Order {order_uuid} completed but no execution. Resetting position to None.")
            return order, None
    else:
        log_with_timestamp(f"[{thread_name}] Buy order placement failed for {ticker}. Response: {response}")
        return None, position

def check_buy_conditions(ticker: str, bid_price: float) -> bool:
    """
    매수 조건을 확인하는 함수

    Parameters
    ----------
    ticker : str
        마켓 코드 (예: "KRW-BTC")
    bid_price : float
        orderbook에서 가져온 매수 가격

    Returns
    -------
    bool
        매수 가능 여부 (True: 매수 가능, False: 매수 불가)
    """
    try:
        # .env에서 설정값 로드
        candle_interval = os.getenv("CANDLE_INTERVAL", "minute60")
        candle_count = int(os.getenv("CANDLE_COUNT", "24"))
        percentile_threshold = float(os.getenv("PERCENTILE_THRESHOLD", "70"))

        # 캔들 데이터 가져오기
        df = get_candles(ticker, interval=candle_interval, count=candle_count)
        if df is None or df.empty:
            log_with_timestamp(f"Warning: No candle data available for {ticker}")
            return False

        # 백분위 가격 계산
        percentile_price = calculate_percentile(df, percentile_threshold)
        if percentile_price is None:
            log_with_timestamp(f"Warning: Could not calculate {percentile_threshold}th percentile for {ticker}")
            return False

        # 매수 조건 확인 (orderbook의 매수 가격이 백분위 가격보다 낮거나 같을 때)
        can_buy = bid_price <= percentile_price

        # 연속 하락 여부 확인
        if can_buy:
            # 1분봉 5개 데이터 가져오기
            one_min_df = get_candles(ticker, interval="minute1", count=5)
            if one_min_df is not None and not one_min_df.empty:
                # 모든 캔들이 하락인지 확인 (open > close)
                all_down = all(one_min_df['open'] > one_min_df['close'])
                if all_down:
                    log_with_timestamp(f"Warning: Last 5 1-minute candles are all down for {ticker}. Skipping buy.")
                    can_buy = False

        # 로깅
        log_with_timestamp(f"\n=== Buy Condition Check for {ticker} ===")
        log_with_timestamp(f"Orderbook Bid Price: {bid_price:,.2f}")
        log_with_timestamp(f"{percentile_threshold}th Percentile Price: {percentile_price:,.2f}")
        log_with_timestamp(f"Can Buy: {can_buy}")

        return can_buy

    except Exception as e:
        log_with_timestamp(f"Error checking buy conditions for {ticker}: {e}")
        return False

def check_emergency_sell_conditions(ticker: str) -> bool:
    """
    긴급 매도(손절) 조건을 확인하는 함수

    Parameters
    ----------
    ticker : str
        마켓 코드 (예: "KRW-BTC")

    Returns
    -------
    bool
        긴급 매도 필요 여부 (True: 긴급 매도 필요, False: 긴급 매도 불필요)
    """
    try:
        # 1분봉 5개 데이터 가져오기
        one_min_df = get_candles(ticker, interval="minute1", count=5)
        if one_min_df is not None and not one_min_df.empty:
            # 모든 캔들이 하락인지 확인 (open > close)
            all_down = all(one_min_df['open'] > one_min_df['close'])
            if all_down:
                log_with_timestamp(f"\n=== Emergency Sell Condition Detected for {ticker} ===")
                log_with_timestamp("Last 5 1-minute candles are all down. Detailed candle information:")

                # 각 캔들의 정보를 시간순으로 출력
                candle_info = []
                for idx, row in one_min_df.iterrows():
                    candle_msg = f"Time: {idx}\n  Open: {row['open']:,.2f}\n  Close: {row['close']:,.2f}\n  Change: {row['close'] - row['open']:,.2f} ({((row['close'] - row['open']) / row['open'] * 100):,.2f}%)"
                    log_with_timestamp(candle_msg)
                    candle_info.append(candle_msg)

                # 디스코드 알림 전송 (로그는 그대로 유지)
                notification_title = f"🚨 Emergency Sell Alert: {ticker}"
                notification_message = f"**Emergency Sell Condition Detected!**\n\nLast 5 1-minute candles are all down:\n\n" + "\n\n".join(candle_info)
                send_discord_notification(notification_message, notification_title)

                return True
        return False
    except Exception as e:
        log_with_timestamp(f"Error checking emergency sell conditions for {ticker}: {e}")
        return False

def trade_continuously(bithumb_api_client, ticker, trade_amount, action_delay_seconds=1):
    thread_name = threading.current_thread().name
    log_with_timestamp(f"[{thread_name}] Starting continuous trading for {ticker}. Action delay: {action_delay_seconds}s")
    
    current_position = None  # None: 초기 상태, "buy": 매수 포지션, "sell": 매도 포지션
    last_buy_price = None
    last_buy_volume = None  # 마지막 매수 수량 저장

    # 스레드에 last_buy_price 저장
    threading.current_thread().last_buy_price = None

    while True:
        try:
            if current_position == "buy": # 매도 시도
                log_with_timestamp(f"[{thread_name}] Current position for {ticker} is 'buy'. Attempting to sell.")

                # 긴급 매도 조건 확인
                if check_emergency_sell_conditions(ticker):
                    log_with_timestamp(f"[{thread_name}] Emergency sell conditions met for {ticker}. Attempting market sell.")
                    try:
                        # 시장가 매도 주문
                        response = bithumb_api_client.sell_market_order(ticker, last_buy_volume)
                        if response and response.get('uuid'):
                            order_uuid = response['uuid']
                            order = bithumb_api_client.get_order(order_uuid)

                            # 거래 정보 로깅
                            executed_volume = float(order.get('executed_volume', 0))
                            # trades 배열에서 평균 가격 계산
                            trades = order.get('trades', [])
                            if trades:
                                total_funds = sum(float(trade['funds']) for trade in trades)
                                executed_price = total_funds / executed_volume if executed_volume > 0 else 0
                            else:
                                executed_price = 0

                            log_with_timestamp(f"\n=== Emergency Sell Details for {ticker} ===")
                            log_with_timestamp(f"Buy Price: {last_buy_price:,.2f}")
                            log_with_timestamp(f"Sell Price: {executed_price:,.2f}")
                            log_with_timestamp(f"Volume: {executed_volume:,.8f}")

                            if last_buy_price is not None:
                                loss_amount = (last_buy_price - executed_price) * executed_volume
                                log_with_timestamp(f"Loss Amount: {loss_amount:,.2f} KRW")

                                # 디스코드 알림 전송 (로그는 그대로 유지)
                                notification_title = f"⚠️ Emergency Market Sell Executed: {ticker}"
                                notification_message = (
                                    f"**Emergency Market Sell Details**\n\n"
                                    f"Buy Price: {last_buy_price:,.2f}\n"
                                    f"Sell Price: {executed_price:,.2f}\n"
                                    f"Volume: {executed_volume:,.8f}\n"
                                    f"Loss Amount: {loss_amount:,.2f} KRW"
                                )

                                # 큰 손실 발생 시 추가 알림
                                if loss_amount > 0:  # 손실 발생
                                    notification_title = f"💔 Loss Alert: {ticker}"

                                send_discord_notification(notification_message, notification_title)

                            # 매도 성공 시에만 포지션 초기화
                            current_position = None
                            last_buy_price = None
                            last_buy_volume = None
                            threading.current_thread().last_buy_price = None
                            threading.current_thread().last_buy_volume = None
                            continue
                    except Exception as e:
                        log_with_timestamp(f"[{thread_name}] Error during emergency sell for {ticker}: {e}")
                        # 에러 발생 시에도 buy 포지션 유지
                        time.sleep(action_delay_seconds)
                        continue

                orderbook = python_bithumb.get_orderbook(ticker)
                if not orderbook or not orderbook.get('orderbook_units'):
                    log_with_timestamp(f"[{thread_name}] Failed to fetch orderbook for {ticker} to sell. Retrying after delay...")
                    time.sleep(action_delay_seconds)
                    continue
                
                price_for_sell = orderbook['orderbook_units'][0]['ask_price']
                log_with_timestamp(f"[{thread_name}] Current ask price for {ticker}: {price_for_sell}")

                proceed_with_sell = False
                if last_buy_price is not None:
                    if float(price_for_sell) > last_buy_price:
                        log_with_timestamp(f"[{thread_name}] Sell condition met for {ticker}: Sell price {price_for_sell} > Last buy price {last_buy_price}")
                        proceed_with_sell = True
                    else:
                        log_with_timestamp(f"[{thread_name}] Sell condition NOT met for {ticker}: Sell price {price_for_sell} < Last buy price {last_buy_price}. Holding 'buy' position.")
                else:
                    # 이전에 매수한 기록이 없는데 포지션이 'buy'인 경우는 논리적으로 발생하기 어려우나, 방어적으로 매도 시도
                    log_with_timestamp(f"[{thread_name}] Warning: Position is 'buy' but no last_buy_price for {ticker}. Attempting sell anyway.")
                    proceed_with_sell = True 

                if proceed_with_sell:
                    final_order, new_position = place_sell_order_and_wait(bithumb_api_client, ticker, price_for_sell, trade_amount)
                    if final_order and new_position == "sell":
                        current_position = "sell"
                        log_with_timestamp(f"[{thread_name}] Sell successful for {ticker}. New position: {current_position}")
                    elif final_order and new_position is None:
                        # emergency sell 등으로 포지션이 초기화된 경우
                        current_position = None
                        last_buy_price = None
                        last_buy_volume = None
                        threading.current_thread().last_buy_price = None
                        threading.current_thread().last_buy_volume = None
                        log_with_timestamp(f"[{thread_name}] Emergency sell or forced position reset. New position: {current_position}")
                    else:
                        log_with_timestamp(f"[{thread_name}] Sell attempt for {ticker} failed or did not complete as expected. Retrying after delay...")
                        # current_position은 "buy"로 유지하고 재시도
                # else: 매도 조건 안맞으면 current_position "buy" 유지

            elif current_position == "sell" or current_position is None: # 매수 시도
                action_type = "Initial Buy" if current_position is None else "Buy (after sell)"
                log_with_timestamp(f"[{thread_name}] Current position for {ticker} is '{current_position}'. Attempting {action_type}.")

                orderbook = python_bithumb.get_orderbook(ticker)
                if not orderbook or not orderbook.get('orderbook_units'):
                    log_with_timestamp(f"[{thread_name}] Failed to fetch orderbook for {ticker} to buy. Retrying after delay...")
                    time.sleep(action_delay_seconds)
                    continue
                
                price_for_buy = orderbook['orderbook_units'][0]['bid_price']
                log_with_timestamp(f"[{thread_name}] Current bid price for {ticker} (maker): {price_for_buy}")

                # 매수 조건 확인 (orderbook의 매수 가격과 백분위 가격 비교)
                if not check_buy_conditions(ticker, float(price_for_buy)):
                    log_with_timestamp(f"[{thread_name}] Buy conditions not met for {ticker}. Waiting for next check...")
                    time.sleep(action_delay_seconds)
                    continue

                final_order, new_position = place_buy_order_and_wait(bithumb_api_client, ticker, price_for_buy, trade_amount)
                if final_order and new_position == "buy":
                    current_position = "buy"
                    try:
                        # trades 배열에서 평균 가격 계산
                        trades = final_order.get('trades', [])
                        executed_volume = float(final_order.get('executed_volume', 0))

                        if trades and executed_volume > 0:
                            total_funds = sum(float(trade['funds']) for trade in trades)
                            last_buy_price = total_funds / executed_volume
                            log_with_timestamp(f"[{thread_name}] Stored last buy price for {ticker}: {last_buy_price:,.2f} (calculated from trades)")
                        else:
                            # trades 정보가 없는 경우 주문 가격을 사용
                            order_price_str = final_order.get('price')
                            if order_price_str:
                                last_buy_price = float(order_price_str)
                                log_with_timestamp(f"[{thread_name}] Stored last buy price for {ticker}: {last_buy_price:,.2f} (from order price as fallback)")
                            else:
                                log_with_timestamp(f"[{thread_name}] Warning: Could not determine executed price for {ticker} from order details.")

                        # 매수 수량 저장
                        if executed_volume > 0:
                            last_buy_volume = executed_volume
                            threading.current_thread().last_buy_volume = executed_volume
                            log_with_timestamp(f"[{thread_name}] Stored last buy volume for {ticker}: {last_buy_volume}")
                    except (ValueError, TypeError, KeyError) as e:
                        log_with_timestamp(f"[{thread_name}] Error parsing price from buy order for {ticker}: {e}")
                    log_with_timestamp(f"[{thread_name}] Buy successful for {ticker}. New position: {current_position}, Last buy price: {last_buy_price}")
                else:
                    log_with_timestamp(f"[{thread_name}] Buy attempt for {ticker} failed or did not complete as expected. Retrying after delay...")
                    # current_position은 이전 상태("sell" or None) 유지하고 재시도
            else:
                # 논리적으로 도달해서는 안되는 상태
                log_with_timestamp(f"[{thread_name}] Unexpected position '{current_position}' for {ticker}. Resetting. Retrying after delay...")
                current_position = None # 안전하게 초기화

            log_with_timestamp(f"[{thread_name}] End of action for {ticker}. Current position: {current_position}, Last buy price: {last_buy_price}. Waiting for {action_delay_seconds}s...")
            time.sleep(action_delay_seconds)

        except Exception as e:
            log_with_timestamp(f"[{thread_name}] An error occurred in trading loop for {ticker}: {e}. Retrying after delay...")
            time.sleep(action_delay_seconds)

def get_candles(ticker: str, interval: str = "day", count: int = 200):
    """
    캔들차트 데이터를 가져오는 함수

    Parameters
    ----------
    ticker : str
        마켓 코드 (예: "KRW-BTC")
    interval : str, optional
        캔들 간격 ("day", "week", "month", "minute1", "minute3", "minute5", "minute10", "minute15", "minute30", "minute60", "minute240")
    count : int, optional
        가져올 캔들 개수 (최대 200)

    Returns
    -------
    pandas.DataFrame
        캔들 데이터가 담긴 DataFrame. 컬럼: open, high, low, close, volume, value
    """
    try:
        df = python_bithumb.get_ohlcv(ticker, interval=interval, count=count)
        if df.empty:
            log_with_timestamp(f"Warning: No candle data returned for {ticker}")
            return None
        return df
    except Exception as e:
        log_with_timestamp(f"Error fetching candles for {ticker}: {e}")
        return None

def calculate_percentile(df, percentile: float):
    """
    캔들 데이터의 종가를 기준으로 특정 백분위의 가격을 계산하는 함수

    Parameters
    ----------
    df : pandas.DataFrame
        get_candles() 함수로 가져온 캔들 데이터
    percentile : float
        계산할 백분위 (0-100 사이의 값)

    Returns
    -------
    float or None
        해당 백분위의 가격. 데이터가 없거나 오류 발생 시 None 반환
    """
    try:
        if df is None or df.empty:
            return None

        # 종가(close) 기준으로 정렬
        sorted_prices = np.sort(df['close'].values)

        # 백분위 계산
        percentile_index = int(len(sorted_prices) * (percentile / 100))
        if percentile_index >= len(sorted_prices):
            percentile_index = len(sorted_prices) - 1

        return float(sorted_prices[percentile_index])
    except Exception as e:
        log_with_timestamp(f"Error calculating {percentile}th percentile: {e}")
        return None

def send_discord_notification(message: str, title: str = None):
    """
    디스코드 웹훅을 통해 알림을 전송하는 함수

    Parameters
    ----------
    message : str
        전송할 메시지 내용
    title : str, optional
        메시지 제목 (기본값: None)
    """
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        log_with_timestamp("Warning: DISCORD_WEBHOOK_URL not set in environment variables")
        return

    try:
        # 임베드 형식으로 메시지 구성
        embed = {
            "title": title if title else "Trading Bot Notification",
            "description": message,
            "color": 3447003,  # 파란색
            "timestamp": datetime.now().isoformat()
        }

        payload = {
            "embeds": [embed]
        }

        headers = {
            "Content-Type": "application/json"
        }

        response = requests.post(webhook_url, json=payload, headers=headers)
        if response.status_code != 204:  # Discord webhook returns 204 on success
            log_with_timestamp(f"Failed to send Discord notification. Status code: {response.status_code}")
    except Exception as e:
        log_with_timestamp(f"Error sending Discord notification: {e}")

def main():
    bithumb_api_client = Bithumb(os.getenv("BITHUMB_ACCESS_KEY"), os.getenv("BITHUMB_SECRET_KEY"))
    
    # .env 파일에서 거래 관련 설정값 로드
    usdt_trade_amount = float(os.getenv("USDT_TRADE_AMOUNT", "10")) # 기본값 10 USDT
    xrp_trade_amount = float(os.getenv("XRP_TRADE_AMOUNT", "10"))   # 기본값 10 XRP
    btc_trade_amount = float(os.getenv("BTC_TRADE_AMOUNT", "0.001")) # 기본값 0.001 BTC
    moodeng_trade_amount = float(os.getenv("MOODENG_TRADE_AMOUNT", "100")) # 기본값 100 MOODENG
    eth_trade_amount = float(os.getenv("ETH_TRADE_AMOUNT", "0.1")) # 기본값 0.1 ETH
    wct_trade_amount = float(os.getenv("WCT_TRADE_AMOUNT", "100")) # 기본값 0.1 ETH
    ondo_trade_amount = float(os.getenv("ONDO_TRADE_AMOUNT", "100")) # 기본값 0.1 ETH
    ada_trade_amount = float(os.getenv("ADA_TRADE_AMOUNT", "100")) # 기본값 0.1 ETH
    # 각 스레드 내의 개별 매수/매도 액션 후 대기 시간
    # API 호출 빈도 및 시장 상황에 맞춰 조절 필요
    action_delay_seconds = int(os.getenv("ACTION_DELAY_SECONDS", "1")) # 기본값 1초

    log_with_timestamp("Starting continuous multi-threaded trading bot...")
    
    # 거래할 자산 목록 생성 (거래량이 0보다 큰 자산만 포함)
    trading_assets = []
    if usdt_trade_amount > 0:
        trading_assets.append(("USDT-Trader", "KRW-USDT", usdt_trade_amount))
    if xrp_trade_amount > 0:
        trading_assets.append(("XRP-Trader", "KRW-XRP", xrp_trade_amount))
    if btc_trade_amount > 0:
        trading_assets.append(("BTC-Trader", "KRW-BTC", btc_trade_amount))
    if moodeng_trade_amount > 0:
        trading_assets.append(("MOODENG-Trader", "KRW-MOODENG", moodeng_trade_amount))
    if eth_trade_amount > 0:
        trading_assets.append(("ETH-Trader", "KRW-ETH", eth_trade_amount))
    if wct_trade_amount > 0:
        trading_assets.append(("WCT-Trader", "KRW-WCT", wct_trade_amount))
    if ondo_trade_amount > 0:
        trading_assets.append(("ONDO-Trader", "KRW-ONDO", ondo_trade_amount))
    if ada_trade_amount > 0:
        trading_assets.append(("ADA-Trader", "KRW-ADA", ada_trade_amount))

    # 거래할 자산이 없는 경우
    if not trading_assets:
        log_with_timestamp("No trading assets configured. Please set trade amounts greater than 0 in .env file.")
        return

    # 거래 설정 로깅
    log_message = "Trading configuration:"
    for _, ticker, amount in trading_assets:
        log_message += f"\n{ticker}: {amount}"
    log_message += f"\nAction Delay: {action_delay_seconds}s"
    log_with_timestamp(log_message)

    # 거래 스레드 생성 및 시작
    trading_threads = []
    for thread_name, ticker, amount in trading_assets:
        thread = threading.Thread(
            target=trade_continuously,
            args=(bithumb_api_client, ticker, amount, action_delay_seconds),
            name=thread_name
        )
        trading_threads.append(thread)
        thread.start()
        time.sleep(1)  # 각 스레드 시작 사이에 1초 딜레이

    try:
        # 모든 스레드가 종료될 때까지 대기
        for thread in trading_threads:
            thread.join()
    except KeyboardInterrupt:
        log_with_timestamp("\nBot stopping due to KeyboardInterrupt...")
        # 스레드가 정상적으로 종료될 시간을 줄 수도 있지만, 데몬 스레드가 아니므로 
        # 주 프로그램 종료 시 강제 종료될 수 있음.
        # 실제 서비스에서는 스레드에 종료 신호를 보내고 join하는 방식이 더 안전합니다.

    log_with_timestamp("All trading threads have been signaled to stop or script interrupted.")

if __name__ == "__main__":
    main()

    
