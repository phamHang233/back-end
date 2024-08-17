import time

from job.algorthisms.average_strategy import AverageStrategy
from job.algorthisms.complete_search import CompleteSearch
from job.algorthisms.genetic_algorithms import GeneticAlgorithms
from job.algorthisms.gradient_descent import GradientDescent
from job.crawlers.uni_pool_data import pool_by_id, get_pool_hour_data, get_pool_day_datas

if __name__ == '__main__':
    pool = '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640'
    # pool = '0xcbcdf9626bc03e24f779434178a73a0b4bad62ed'
    protocol = 'ethereum'
    end_timestamp = int(time.time())
    start_timestamp = end_timestamp - 30 * 24 * 3600
    # ga = GeneticAlgorithms(pool=pool, protocol=protocol, start_timestamp=start_timestamp,
    #                        end_timestamp=end_timestamp)
    # data, best_range = ga.process()
    # print(best_range, data)
    pool_data = pool_by_id(pool, "ethereum")
    hourly_price_data = get_pool_hour_data(pool, start_timestamp, end_timestamp, "ethereum")

    pool_day_info = get_pool_day_datas(pool, "ethereum", from_date=start_timestamp, to_date=end_timestamp)

    average_s = AverageStrategy(pool, pool_data, hourly_price_data[2:], pool_day_info[:28])
    print(average_s.process())
    # gd = GradientDescent(pool=pool, start_timestamp=start_timestamp, end_timestamp=end_timestamp)
    # optimal_min_range, optimal_max_range = gd.gradient_descent()
    # print(optimal_min_range, optimal_max_range)

    # pool_info = get_pool_day_datas(pool, protocol, from_date=start_timestamp,
    #                                          to_date=end_timestamp)
    # #
    # lowest_price = min([float(pool_info[i]['low']) for i in range(len(pool_info))])
    # highest_price = max([float(pool_info[i]['high']) for i in range(len(pool_info))])
    # #
    # sa = SimulatedAnneal(pool=pool,protocol=protocol, min_price_bounds=(lowest_price, highest_price),
    #                      max_price_bounds=(lowest_price, highest_price), iterations=100,  cooling_rate=0.95)
    # best_min_price, best_max_price, best_apr = sa.simulated_annealing()
    # # In ra kết quả
    # print(f"Giá trị tối ưu: min_price = {best_min_price:.2f}, max_price = {best_max_price:.2f}, APR = {best_apr:.4f}")
    # test= CompleteSearch(pool=pool, start_timestamp=start_timestamp, end_timestamp=end_timestamp)
    # test.process()
