import unittest
from bot.bot import send_discord_notification
import os
from dotenv import load_dotenv

class TestDiscordNotification(unittest.TestCase):
    def setUp(self):
        # .env 파일 로드
        load_dotenv()
        # 웹훅 URL이 설정되어 있는지 확인
        self.webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
        if not self.webhook_url:
            self.skipTest("DISCORD_WEBHOOK_URL not set in environment variables")

    def test_basic_notification(self):
        """기본 알림 전송 테스트"""
        message = "This is a test notification"
        title = "Test Notification"
        send_discord_notification(message, title)

    def test_emergency_alert(self):
        """긴급 알림 형식 테스트"""
        message = (
            "**Emergency Sell Condition Detected!**\n\n"
            "Last 5 1-minute candles are all down:\n\n"
            "Time: 2025-05-22 10:00:00\n"
            "  Open: 3,500.00\n"
            "  Close: 3,400.00\n"
            "  Change: -100.00 (-2.86%)\n\n"
            "Time: 2025-05-22 10:01:00\n"
            "  Open: 3,400.00\n"
            "  Close: 3,300.00\n"
            "  Change: -100.00 (-2.94%)"
        )
        title = "🚨 Emergency Sell Alert: KRW-XRP"
        send_discord_notification(message, title)

    def test_loss_alert(self):
        """손실 알림 형식 테스트"""
        message = (
            "**Emergency Market Sell Details**\n\n"
            "Original Limit Order Price: 3,500.00\n"
            "Market Sell Price: 3,300.00\n"
            "Volume: 100.00000000\n"
            "Loss Amount: 20,000.00 KRW"
        )
        title = "💔 Loss Alert: KRW-XRP"
        send_discord_notification(message, title)

    def test_long_message(self):
        """긴 메시지 테스트"""
        message = "This is a very long test message " * 10
        title = "Long Message Test"
        send_discord_notification(message, title)

    def test_special_characters(self):
        """특수문자 포함 메시지 테스트"""
        message = "Test with special characters: 🚨 ⚠️ 💔 📈 📉"
        title = "Special Characters Test"
        send_discord_notification(message, title)

if __name__ == '__main__':
    unittest.main() 