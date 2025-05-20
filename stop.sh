#!/bin/bash

PROCESS_NAME="python3 -u bot.py"

echo "Attempting to stop process: $PROCESS_NAME"

# pkill 명령어의 -f 옵션은 전체 명령어 라인과 일치하는 프로세스를 찾습니다.
pkill -f "$PROCESS_NAME"

# 종료 상태 확인
if [ $? -eq 0 ]; then
  echo "$PROCESS_NAME process(es) stopped successfully."
else
  # pkill은 일치하는 프로세스가 없으면 1을 반환합니다.
  # 다른 오류 코드일 수도 있습니다.
  echo "No $PROCESS_NAME process found or an error occurred while trying to stop it."
fi
