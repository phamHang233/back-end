import json

from app.utils.format_utils import pretty_tx_method
from app.utils.singleton import SingletonMeta


class SignaturesConstants(metaclass=SingletonMeta):
    def __init__(self):
        with open('app/services/artifacts/signatures.json') as json_file:
            print('init')
            self.data = json.load(json_file)

    def get_method(self, string, default='Called', pretty=True):
        sign = string[2:10]
        if not sign:
            return 'Transfer'
        method = self.data.get(sign, default)
        if pretty and method:
            method = pretty_tx_method(method)
        return method
