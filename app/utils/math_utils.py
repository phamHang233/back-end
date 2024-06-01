import time


def to_int(value):
    if value is not None:
        value = int(value)

    return value


def divide(a, b, default=None):
    if b:
        if a == 0:
            return 0
        elif not a:
            return default
        else:
            return a / b
    else:
        if a == 0:
            return 0
        else:
            return default


def divide_diricle(n_rabbits: int, n_cages: int):
    if n_cages == 0:
        return []

    love = n_rabbits // n_cages
    res = n_rabbits % n_cages
    rabbits_per_cage = [love] * n_cages
    for idx in range(res):
        rabbits_per_cage[idx] += 1
    return rabbits_per_cage


def short_histogram(histogram, interval=1):
    histogram = {int(t): v for t, v in histogram.items()}
    histogram = dict(sorted(histogram.items(), reverse=False))

    min_value = int(min(histogram) / interval) * interval
    max_value = int(max(histogram) / interval) * interval

    ranges = list(range(min_value, max_value + 1, interval))
    data = {r: 0 for r in ranges}
    for k, n in histogram.items():
        idx = int(k / interval) * interval
        data[idx] += n

    return data


def get_histogram(values: list, ranges: list, field_name='numberOfValues'):
    if not values or not ranges:
        return []

    values = sorted(values)

    if not ranges:
        return [{
            'startValue': values[0],
            'endValue': values[-1],
            field_name: len(values)
        }]

    pre_value = values[0]
    if pre_value < ranges[0]:
        ranges.insert(0, pre_value)

    post_value = values[-1]
    if post_value > ranges[-1]:
        ranges.append(post_value + 1)

    histogram = []
    for idx, s in enumerate(ranges[:-1]):
        t = ranges[idx + 1]
        n = len([v for v in values if s <= v < t])
        histogram.append({
            'startValue': s,
            'endValue': t,
            field_name: n
        })
    return histogram


def sum_with_none(*elements, allow_none=False, default_value=0, default_return=None):
    if (not allow_none) and any([e is None for e in elements]):
        return default_return

    return sum([e or default_value for e in elements])


def get_average(log: dict, start_time=None, current_time=None):
    if not log:
        return 0

    out_idx = None
    if start_time is None:
        start_time = list(log.keys())[0]
        out_idx = 0

    if current_time is None:
        current_time = int(time.time())

    timestamps = list(log.keys())
    values = list(log.values())

    out_time_idx = None
    average = 0
    for idx in range(0, len(timestamps) - 1):
        if timestamps[idx] < start_time:
            out_idx = idx
        elif timestamps[idx] > current_time:
            out_time_idx = idx
            break
        else:
            average += values[idx] * (timestamps[idx + 1] - timestamps[idx])

    if timestamps[-1] < start_time:
        out_idx = len(timestamps) - 1
    elif (out_time_idx is None) and (timestamps[-1] > current_time):
        out_time_idx = len(timestamps) - 1

    if out_idx is not None:
        next_timestamp = timestamps[out_idx + 1] if out_idx < len(timestamps) - 1 else start_time
        average += values[out_idx] * (next_timestamp - start_time)

    if out_time_idx is None:
        last_timestamp = max(timestamps[-1], start_time)
        average += values[-1] * (current_time - last_timestamp)
    elif out_time_idx > 0:
        average -= values[out_time_idx - 1] * (timestamps[out_time_idx] - current_time)

    average /= current_time - start_time
    return average
