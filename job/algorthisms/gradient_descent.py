import random
import time

import numpy as np

from job.crawlers.uni_pool_data import get_pool_day_datas, get_pool_hour_data, pool_by_id
from job.services.fee_from_strategy import uniswap_strategy_algorithm
from job.utils.sqrt_price_math import convert_price_to_tick


class GradientDescent:
    # Hàm calculate_apr(min_range, max_range) giả định
    def __init__(self, pool, start_timestamp, end_timestamp, protocol='ethereum'):
        self.max_tick = 0
        self.min_tick = 0
        self.pool = pool
        self.protocol = protocol
        self.generations = 4  # Số thế hệ
        self.crossover_rate = 0.8  # Tỷ lệ giao phối
        self.mutation_rate = 0.3  # Tỷ lệ đột biến
        self.num_parents = 20
        # Tạo quần thể ban đầu
        self.pool_data = pool_by_id(pool, protocol)
        self.population_size = 100
        self.end_timestamp = end_timestamp
        self.start_timestamp = start_timestamp
        self.pool_hour_data = get_pool_hour_data(pool, start_timestamp, end_timestamp, protocol)[::-1]

    def calculate_apr(self, min_tick, max_tick):
        # Hàm giả định, cần thay bằng hàm thực tế của bạn
        data = uniswap_strategy_algorithm(
            pool_data=self.pool_data, backtest_data=self.pool_hour_data, investment_amount=1000,
            min_tick=min_tick * 10, max_tick=max_tick * 10)

        return data['apr'] * 100

    # Gradient số của hàm calculate_apr
    def numerical_gradient(self, min_range, max_range, h=1e-5):
        grad = np.zeros(2)
        apr_current = self.calculate_apr(min_range, max_range)

        # Gradient theo min_range
        min_range_plus_h = self.calculate_apr(min_range + h, max_range)
        grad[0] = (min_range_plus_h - apr_current) / h

        # Gradient theo max_range
        max_range_plus_h = self.calculate_apr(min_range, max_range + h)
        grad[1] = (max_range_plus_h - apr_current) / h

        return grad


    def gradient_descent(self, lr=2, num_iterations=150):
        # Khởi tạo min_range và max_range ngẫu nhiên
        self.pool_info = get_pool_day_datas(self.pool, self.protocol, from_date=self.start_timestamp,
                                            to_date=self.end_timestamp)
        min_price = min([float(self.pool_info[i]['low']) for i in range(len(self.pool_info))])
        max_price = max([float(self.pool_info[i]['high']) for i in range(len(self.pool_info))])
        decimals0 = int(self.pool_data["token0"]['decimals'])
        decimals1 = int(self.pool_data["token1"]['decimals'])

        self.current_tick = convert_price_to_tick(float(self.pool_hour_data[-1]['close']), decimals0, decimals1) / 10
        self.min_tick = convert_price_to_tick(max_price, decimals0, decimals1) / 10
        self.max_tick = convert_price_to_tick(min_price, decimals0, decimals1) / 10
        # tick_lower = int(random.uniform(self.min_tick, self.current_tick))
        # tick_upper = int(random.uniform(self.current_tick + 1, self.max_tick))
        tick_lower = self.min_tick
        tick_upper = self.max_tick
        start_time = time.time()
        for i in range(num_iterations):
            # Tính gradient số tại điểm hiện tại
            grad = self.numerical_gradient(tick_lower, tick_upper)

            # Cập nhật min_range và max_range
            tick_lower += lr * grad[0]
            tick_upper += lr * grad[1]
            # Đảm bảo tick_lower luôn nhỏ hơn tick_upper và nằm trong phạm vi hợp lệ
            tick_lower = min(tick_lower, tick_upper - 1)  # tick_lower không được vượt quá tick_upper
            tick_lower = max(self.min_tick, min(tick_lower, self.max_tick))  # Giới hạn giá trị cho tick_lower
            tick_upper = max(tick_lower + 1, tick_upper)  # tick_upper phải lớn hơn tick_lower
            tick_upper = max(self.min_tick, min(tick_upper, self.max_tick))  # Giới hạn giá trị cho tick_upper

            # In ra giá trị hiện tại của hàm calculate_apr và các tham số
            apr = self.calculate_apr(tick_lower, tick_upper)
            print(f"Iteration {i + 1}: min_tick = {tick_lower * 10}, max_tick = {tick_upper * 10}, APR = {apr}")
        print(f'time: {time.time() - start_time}')
        return tick_lower, tick_upper

