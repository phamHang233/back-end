import re
from typing import Optional

import base58


def short_address(address: str, n_start=3, n_end=4):
    if not address:
        return ''
    return address[:n_start + 2] + '...' + address[-n_end:]


def pretty_tx_method(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1 \2', name)
    name = re.sub('([a-z0-9])([A-Z])', r'\1 \2', name)
    return name[:1].upper() + name[1:]


def zero_to_none(d: dict, zero_values: Optional[list] = None, zero_types: Optional[list] = None):
    if zero_types is None:
        zero_types = []

    if zero_values is None:
        zero_values = []

    zero_values.extend([type_() for type_ in zero_types])
    if not zero_values:
        zero_values = [0]

    for k in d.keys():
        if d[k] in zero_values:
            d[k] = None


def about(value, _min=300, _max=850):
    if value < _min:
        value = _min
    elif value > _max:
        value = _max
    return int(value)


def hex_to_base58(hex_string):
    if hex_string[:2] in ["0x", "0X"]:
        hex_string = "41" + hex_string[2:]
    bytes_str = bytes.fromhex(hex_string)
    base58_str = base58.b58encode_check(bytes_str)
    return base58_str.decode("UTF-8")


def base58_to_hex(base58_string):
    asc_string = base58.b58decode_check(base58_string)
    hex_str = asc_string.hex().upper()
    return '0x' + hex_str[2:].lower()


def convert_currency_format(amount):
    amount = int(amount)
    if abs(amount) >= 1e9:
        # Chuyển đổi thành tỷ (Billion)
        formatted_amount = f'{amount / 1e9:.2f}B'
    elif abs(amount) >= 1e6:
        # Chuyển đổi thành triệu (Million)
        formatted_amount = f'{amount / 1e6:.2f}M'
    elif abs(amount) >= 1e3:
        formatted_amount = f'{amount / 1e3:.2f}K'
    else:
        # Giữ nguyên nếu không đạt đến ngưỡng chuyển đổi
        formatted_amount = f'{amount:,.0f}'

    return formatted_amount
