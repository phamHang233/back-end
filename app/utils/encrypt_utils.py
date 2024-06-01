# import hashlib
#
# from sanic.exceptions import BadRequest
# from web3 import Web3
#
# KEY = 'dev-123'
#
#
# def hash_id(ids):
#     string = ':'.join(ids)
#     return hashlib.sha256(f'{string}:{KEY}'.encode()).hexdigest()
#
#
# def check_address(address: str):
#     if not Web3.isAddress(address):
#         raise BadRequest(f'Invalid wallet address: {address}')
#     return address.lower()
