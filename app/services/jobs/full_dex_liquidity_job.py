import time

from query_state_lib.client.client_querier import ClientQuerier
from web3 import Web3

from app.services.jobs.multicall_v2 import W3Multicall, add_rpc_multicall, decode_multical_response
from app.utils.logger_utils import get_logger
from job.artifacts.abis.pool_v3_abi import UNISWAP_V3_POOL_ABI

logger = get_logger("Get liquidity in all Tick")


class LiquidityPoolEnricher:
    def __init__(self, pool, provider_uri):
        self.pool = pool
        self.client_querier = ClientQuerier(provider_url=provider_uri)
        self._w3 = Web3(Web3.HTTPProvider(provider_uri))
        self.multicall_address = Web3.to_checksum_address("0xca11bde05977b3631167028862be2a173976ca11")
        self.w3_multicall = W3Multicall(self._w3, address=self.multicall_address)

    def _start(self):
        self.liquidity = {}
        self.current_tick = None

    def _execute(self):
        start_time = time.time()
        response_data = self.prepare_enrich()
        self.process_data(response_data)
        logger.info(f"Get liquidity in {time.time() - start_time}")

    def prepare_enrich(self):
        self.w3_multicall.add(W3Multicall.Call(
            abi=UNISWAP_V3_POOL_ABI, fn_name='slot0', address=self.pool, block_number="latest"))
        self.w3_multicall.add(W3Multicall.Call(
            abi=UNISWAP_V3_POOL_ABI, fn_name='tickSpacing', address=self.pool, block_number="latest"))
        self.w3_multicall.add(W3Multicall.Call(
            abi=UNISWAP_V3_POOL_ABI, fn_name='liquidity', address=self.pool, block_number="latest"))

        list_call_id, list_rpc_call = [], []
        add_rpc_multicall(self.w3_multicall, list_rpc_call=list_rpc_call, list_call_id=list_call_id)

        responses = self.client_querier.sent_batch_to_provider(list_rpc_call, batch_size=1)
        decoded_data = decode_multical_response(
            w3_multicall=self.w3_multicall, data_responses=responses,
            list_call_id=list_call_id, ignore_error=True
        )
        tick_spacing = decoded_data.get(f'tickSpacing_{self.pool}_latest'.lower())
        self.tick_space = tick_spacing
        current_tick = decoded_data.get(f'slot0_{self.pool}_latest'.lower())[1]
        round_current_tick = int(current_tick / tick_spacing) * tick_spacing

        for tick in range(round_current_tick - tick_spacing * 200, round_current_tick + tick_spacing * 200,
                          tick_spacing):
            self.w3_multicall.add(W3Multicall.Call(
                self.pool, UNISWAP_V3_POOL_ABI,
                fn_name='ticks', fn_paras=tick, block_number='latest'
            ))

        list_call_id, list_rpc_call = [], []
        add_rpc_multicall(self.w3_multicall, list_rpc_call=list_rpc_call, list_call_id=list_call_id)

        responses = self.client_querier.sent_batch_to_provider(list_rpc_call, batch_size=1)
        decoded_data.update(decode_multical_response(
            w3_multicall=self.w3_multicall, data_responses=responses,
            list_call_id=list_call_id, ignore_error=True
        ))

        return decoded_data

    def process_data(self, response_data):
        tick_spacing = response_data.get(f'tickSpacing_{self.pool}_latest'.lower())
        self.current_tick = response_data.get(f'slot0_{self.pool}_latest'.lower())[1]
        round_current_tick = int(self.current_tick / tick_spacing) * tick_spacing
        initial_liquidity = response_data.get(f'liquidity_{self.pool}_latest'.lower())
        self.liquidity[round_current_tick] = initial_liquidity
        for tick in reversed(range(round_current_tick - tick_spacing * 200, round_current_tick, tick_spacing)):
            liquidity_change = response_data.get(f'ticks_{self.pool}_{tick}_latest'.lower())[1]
            self.liquidity[tick] = self.liquidity[tick + tick_spacing] - liquidity_change

        for tick in range(round_current_tick + tick_spacing, round_current_tick + tick_spacing * 200, tick_spacing):
            liquidity_change = response_data.get(f'ticks_{self.pool}_{tick}_latest'.lower())[1]
            self.liquidity[tick] = self.liquidity[tick - tick_spacing] + liquidity_change

    def run(self):
        self._start()
        self._execute()
