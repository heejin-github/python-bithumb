import unittest
import pandas as pd
import numpy as np
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.bot import get_candles, calculate_percentile, log_with_timestamp

class TestCandleFunctions(unittest.TestCase):
    def setUp(self):
        # 테스트에 사용할 실제 마켓 코드
        self.test_ticker = "KRW-XRP"
        # 모든 컬럼 표시 설정
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.float_format', lambda x: '%.2f' % x)

    def test_get_candles_success(self):
        # 실제 API 호출로 60분봉 데이터 가져오기
        result = get_candles(self.test_ticker, interval="minute60", count=24)
        
        # 검증
        self.assertIsNotNone(result)
        self.assertGreater(len(result), 0)
        
        # 실제 API 응답 컬럼 확인 (get_ohlcv에서 rename된 컬럼명)
        expected_columns = ['market', 'candle_date_time_utc', 'open', 'high', 'low', 'close', 
                          'timestamp', 'value', 'volume', 'unit']
        self.assertEqual(list(result.columns), expected_columns)
        
        # 캔들 데이터 로깅
        log_with_timestamp(f"\n=== 60분봉 캔들 데이터 ({self.test_ticker}) ===")
        log_with_timestamp(f"총 캔들 수: {len(result)}")
        log_with_timestamp(f"기간: {result.index[0]} ~ {result.index[-1]}")
        log_with_timestamp(f"최신 종가: {float(result['close'].iloc[-1]):,.2f}")
        log_with_timestamp(f"최고가: {float(result['high'].max()):,.2f}")
        log_with_timestamp(f"최저가: {float(result['low'].min()):,.2f}")
        log_with_timestamp(f"평균 거래량: {float(result['volume'].mean()):,.2f}")
        
        # DataFrame 전체 출력
        log_with_timestamp("\n=== 상세 캔들 데이터 ===")
        log_with_timestamp(f"\n{result.to_string()}")

    def test_get_candles_different_intervals(self):
        # 다양한 시간 간격으로 테스트
        intervals = ["day", "week", "month", "minute1", "minute5", "minute15", "minute30", "minute60"]
        
        for interval in intervals:
            result = get_candles(self.test_ticker, interval=interval, count=100)
            
            # 검증
            self.assertIsNotNone(result)
            self.assertGreater(len(result), 0)
            
            # 각 간격별 데이터 로깅
            log_with_timestamp(f"\n=== {interval} Candle Data for {self.test_ticker} ===")
            log_with_timestamp(f"Total candles: {len(result)}")
            log_with_timestamp(f"Date range: {result.index[0]} to {result.index[-1]}")
            log_with_timestamp(f"Latest close price: {float(result['close'].iloc[-1]):,.2f}")

    def test_calculate_percentile_with_real_data(self):
        # 실제 데이터로 백분위 계산 테스트
        df = get_candles(self.test_ticker, interval="minute60", count=24)
        self.assertIsNotNone(df)
        
        # 다양한 백분위 테스트
        percentiles = [0, 25, 50, 75, 100]
        log_with_timestamp(f"\n=== Percentile Analysis for {self.test_ticker} ===")
        log_with_timestamp(f"Total data points: {len(df)}")
        
        for p in percentiles:
            result = calculate_percentile(df, p)
            self.assertIsNotNone(result)
            self.assertTrue(isinstance(result, float))
            self.assertGreater(result, 0)
            
            # 백분위 값 로깅
            log_with_timestamp(f"{p}th percentile price: {result:,.2f}")
            
            # 백분위 값이 올바른 순서인지 확인
            if p > 0:
                prev_result = calculate_percentile(df, p-25)
                self.assertGreaterEqual(result, prev_result)
                log_with_timestamp(f"Price difference from previous percentile: {result - prev_result:,.2f}")

    def test_calculate_percentile_edge_cases(self):
        # 실제 데이터로 엣지 케이스 테스트
        df = get_candles(self.test_ticker, interval="minute60", count=24)
        self.assertIsNotNone(df)
        
        # 잘못된 백분위 값 테스트
        result_negative = calculate_percentile(df, -10)
        result_over_100 = calculate_percentile(df, 150)
        
        # 엣지 케이스 결과 로깅
        log_with_timestamp(f"\n=== Edge Cases Analysis for {self.test_ticker} ===")
        log_with_timestamp(f"Negative percentile (-10) result: {result_negative:,.2f}")
        log_with_timestamp(f"Over 100 percentile (150) result: {result_over_100:,.2f}")
        log_with_timestamp(f"Price range: {result_over_100 - result_negative:,.2f}")
        
        # 검증 - 잘못된 값은 가장 가까운 유효한 값으로 처리됨
        self.assertIsNotNone(result_negative)
        self.assertIsNotNone(result_over_100)
        self.assertGreaterEqual(result_over_100, result_negative)

    def test_calculate_percentile_empty_data(self):
        # 빈 DataFrame으로 테스트
        empty_df = pd.DataFrame()
        result = calculate_percentile(empty_df, 50)
        self.assertIsNone(result)
        log_with_timestamp("\n=== Empty Data Test ===")
        log_with_timestamp("Result for empty DataFrame: None")

    def test_calculate_percentile_none_data(self):
        # None 데이터로 테스트
        result = calculate_percentile(None, 50)
        self.assertIsNone(result)
        log_with_timestamp("\n=== None Data Test ===")
        log_with_timestamp("Result for None data: None")

if __name__ == '__main__':
    unittest.main() 