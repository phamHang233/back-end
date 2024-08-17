import threading
import time
from concurrent.futures import ThreadPoolExecutor

from job.crawlers.uni_pool_data import pool_by_id, get_pool_day_datas, get_pool_hour_data
from job.services.fee_from_strategy import uniswap_strategy_algorithm
from job.utils.sqrt_price_math import convert_price_to_tick, convert_tick_to_price


class CompleteSearch:
    def __init__(self, pool, start_timestamp, end_timestamp, protocol='ethereum'):
        self.pool_data = pool_by_id(pool, protocol)
        self.pool_day_data = get_pool_day_datas(pool, protocol, start_timestamp, end_timestamp)
        self.pool_hour_data = get_pool_hour_data(pool, start_timestamp, end_timestamp, protocol)[::-1]

        self.pool = pool
        self.protocol = protocol
        self.end_timestamp = end_timestamp
        self.start_timestamp = start_timestamp
        self.distance = 3

    def cal_best_apr(self, min_tick, max_tick, aprs, ranges):
        print(f"{min_tick}, {max_tick}")
        # start = time.time()
        data = uniswap_strategy_algorithm(
            pool_data=self.pool_data, backtest_data=self.pool_hour_data, investment_amount=1000,
            min_tick=min_tick * 10, max_tick=max_tick * 10)
        apr = data['apr']
        aprs.append(apr)
        ranges.append([min_tick, max_tick])
        # print(f'time toke {time.time() - start}')
        # time.sleep(1)

    def process(self):
        min_price = min([float(self.pool_day_data[i]['low']) for i in range(len(self.pool_day_data))])
        max_price = max([float(self.pool_day_data[i]['high']) for i in range(len(self.pool_day_data))])
        decimals0 = int(self.pool_data['token0']['decimals'])
        decimals1 = int(self.pool_data['token1']['decimals'])

        current_tick = int(convert_price_to_tick(float(self.pool_hour_data[-1]['close']), decimals0, decimals1) / 10)
        self.min_tick = int(convert_price_to_tick(max_price, decimals0, decimals1) / 10)
        self.max_tick = int(convert_price_to_tick(min_price, decimals0, decimals1) / 10)
        print(f'min: {self.min_tick}, max: {self.max_tick}, distance: {self.min_tick - self.max_tick}')
        threads = []
        aprs = []
        ranges = []
        start = time.time()
        # with ThreadPoolExecutor(max_workers=10) as executor:
        #     for tick_lower in range(self.min_tick, current_tick, self.distance):
        #         for tick_upper in range(max(current_tick, tick_lower+self.distance), self.max_tick, self.distance):
        #             executor.submit(self.cal_best_apr, tick_lower, tick_upper, aprs, ranges)
        for tick_lower in range(self.min_tick, current_tick, self.distance):
            for tick_upper in range(max(current_tick, tick_lower+self.distance), self.max_tick, self.distance):
                self.cal_best_apr(tick_lower, tick_upper, aprs, ranges)

        highest_apr = max(aprs)
        best_range = ranges[aprs.index(highest_apr)]
        print(f"RESULT: {best_range}- {highest_apr}")
        print(f"time execute token {time.time() - start}, {len(aprs)}")

# print(convert_tick_to_price(193730, 6, 18 ))
# print(convert_tick_to_price(194840, 6, 18 ))
print(convert_tick_to_price(19443 * 10, 6,18))
print(convert_tick_to_price(19382 *10, 6,18))
