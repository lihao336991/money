from datetime import datetime, time
from zoneinfo import ZoneInfo  # Python 3.9+
import uuid

import pandas as pd
import time

# local_timezone = ZoneInfo("Asia/Shanghai")

# now = datetime.now()
# currentTime = int(now.timestamp() * 1000)
# today = pd.to_datetime(currentTime, unit='ms')


# print(today, today.strftime('%Y%m%d'), now)


# def is_trading():
#     current_time = datetime.now().time()
#     print(current_time)
#     return time(9,0) <= current_time <= time(17,0)


# for i in [1, 2, 3]:
#     print(i)
#     time.sleep(3)

print(str(uuid.uuid4()))