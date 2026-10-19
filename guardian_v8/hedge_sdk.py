"""SDK 0.5.0 adapter for share-exact hedge orders and recoverable merge polling.

Private SDK helpers are confined here and version-checked. They are the SAME
ABI, exchange domain and position routing used by SecureClient, not hardcoded
contract addresses. No network/signing happens at import time.
"""
from dataclasses import asdict
from decimal import Decimal
from importlib.metadata import version


class HedgeSDK:
    def __init__(self, client, rate_wait):
        if version("polymarket-client") != "0.5.0":
            raise RuntimeError("hedge_merge requires polymarket-client==0.5.0; see HEDGE_EXIT.md")
        self.client = client
        self.rate_wait = rate_wait
        self.contexts = {}

    def context(self, condition):
        if condition not in self.contexts:
            self.contexts[condition] = self.client._resolve_market_position_context(
                condition_id=condition)
        return self.contexts[condition]

    def tokens(self, condition):
        return tuple(str(t) for t in self.context(condition).token_ids)

    def balances(self, condition):
        from polymarket._internal.actions.relayer.calls import (
            erc1155_balance_of_batch_call, decode_erc1155_balance_of_batch_result)
        ctx = self.context(condition)
        call = erc1155_balance_of_batch_call(
            token_address=ctx.position_erc1155_address,
            owners=[self.client.wallet, self.client.wallet], token_ids=list(ctx.token_ids))
        result = self.client._ctx.rpc.eth_call(to=str(call.to), data=call.data)
        return tuple(Decimal(b) / 1_000_000
                     for b in decode_erc1155_balance_of_batch_result(result))

    def approved(self, condition):
        from polymarket._internal.actions.relayer.calls import (
            erc1155_is_approved_for_all_call, decode_erc1155_is_approved_for_all_result)
        ctx = self.context(condition)
        call = erc1155_is_approved_for_all_call(
            token_address=ctx.position_erc1155_address,
            owner=self.client.wallet, operator=ctx.adapter_address)
        return decode_erc1155_is_approved_for_all_result(
            self.client._ctx.rpc.eth_call(to=str(call.to), data=call.data))

    def approve(self, condition):
        ctx = self.context(condition)
        self.rate_wait()
        return self.client.approve_erc1155_for_all(
            token_address=str(ctx.position_erc1155_address),
            operator_address=str(ctx.adapter_address), approved=True)

    def prepare(self, condition, token, price, shares, passive, forced):
        signed = self.client.create_limit_order(
            token_id=token, side="BUY", price=str(price), size=str(shares),
            post_only=passive and not forced)
        # Keep GTC even for forced exits. Market BUY/FOK is documented in USD,
        # whereas limit BUY/GTC has an explicit share-size contract. A forced
        # exit uses an aggressive limit, never a cash-budget market order.
        if signed.taker_amount != int(shares * 1_000_000):
            raise ValueError("SDK changed hedge share quantity")
        return asdict(signed), self.order_id(condition, signed)

    def order_id(self, condition, signed):
        from eth_utils import keccak
        from polymarket._internal.actions.orders.context import resolve_exchange_address
        from polymarket._internal.actions.orders.types import UnsignedOrder
        from polymarket._internal.actions.orders.typed_data import (
            _app_domain_separator, _order_contents_hash, _PROTOCOL_VERSION)
        values = asdict(signed)
        values.pop("signature")
        values.pop("post_only")
        env = self.client._ctx.environment_config
        unsigned = UnsignedOrder(
            **values, chain_id=env.chain_id,
            exchange_address=resolve_exchange_address(env, self.context(condition).neg_risk))
        domain = bytes.fromhex(_app_domain_separator(unsigned, protocol_version=_PROTOCOL_VERSION)[2:])
        return "0x" + keccak(b"\x19\x01" + domain + _order_contents_hash(unsigned)).hex()

    def post(self, signed):
        from polymarket.models.clob.orders import SignedOrder
        self.rate_wait()
        return self.client.post_order(SignedOrder(**signed))

    def cancel(self, order_id):
        self.rate_wait()
        return self.client.cancel_order(order_id=order_id)

    def merge(self, condition, shares):
        self.rate_wait()
        return self.client.merge_positions(
            condition_id=condition, amount=int(shares * 1_000_000))

    def transaction_state(self, transaction):
        if transaction.get("id"):
            from polymarket._internal.actions.relayer.poll import fetch_gasless_transaction_sync
            tx = fetch_gasless_transaction_sync(
                self.client._ctx.relayer, transaction_id=transaction["id"])
            return str(tx.state)
        if transaction.get("hash"):
            # EOA fallback uses the SDK's own successful-receipt criterion.
            from polymarket.transactions import SyncEoaTransactionHandle
            from polymarket.errors import TimeoutError, TransactionFailedError
            handle = SyncEoaTransactionHandle(
                transaction_hash=transaction["hash"], _rpc=self.client._ctx.rpc,
                _max_polls=1, _poll_delay_s=0)
            try:
                handle.wait()
                return "STATE_CONFIRMED"
            except TimeoutError:
                return "STATE_MINED"
            except TransactionFailedError:
                return "STATE_FAILED"
        return "UNKNOWN"
