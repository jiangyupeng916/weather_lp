"""Verify supported SDKs' actual amount, hashing, routing and serialization."""
from dataclasses import replace
from decimal import Decimal as D
from importlib.metadata import version
from pathlib import Path
import sys
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hedge_sdk import HedgeSDK
from polymarket.models.clob.orders import SignedOrder

ZERO32 = "0x" + "00" * 32
WALLET = "0x" + "12" * 20
EXCHANGE = "0x" + "34" * 20
NEG_EXCHANGE = "0x" + "56" * 20
V3_EXCHANGE = "0x" + "78" * 20
ROUTER = "0x" + "9a" * 20
LEDGER = "0x" + "ab" * 20
ADAPTER = "0x" + "bc" * 20
NEG_BALANCE_ADAPTER = "0x" + "cd" * 20
NEG_MERGE_OPERATOR = "0x" + "de" * 20
V2_LEDGER = "0x" + "ef" * 20
# Realistic CTF IDs occupy a different namespace from structured V2 IDs.
CTF_TOKEN = str((1 << 200) + (1 << 40) + 123456789)
V2_TOKEN = str((1 << 248) + (42 << 104) + 1)
SDK_VERSION = version("polymarket-client")


def signed():
    return SignedOrder(builder=ZERO32, expiration=0, maker=WALLET,
                       maker_amount=3_200_000, metadata=ZERO32, order_type="GTC", salt=12345,
                       side="BUY", signature="0x" + "00" * 65, signature_type=3,
                       signer=WALLET, taker_amount=20_000_000, timestamp=1_000_000,
                       token_id=CTF_TOKEN, post_only=False)


def adapter():
    client = NS(wallet=WALLET, create_limit_order=Mock(return_value=signed()),
                merge_positions=Mock(), post_order=Mock(),
                _ctx=NS(environment_config=NS(chain_id=137, standard_exchange=EXCHANGE,
                                              neg_risk_exchange=NEG_EXCHANGE,
                                              exchange_v3=V3_EXCHANGE,
                                              protocol_v2_router=ROUTER,
                                              conditional_tokens=LEDGER,
                                              collateral_token=WALLET)),
                _resolve_market_position_context=Mock(return_value=NS(
                    protocol="ctf", neg_risk=False, adapter_address=ADAPTER,
                    position_erc1155_address=LEDGER, token_ids=(CTF_TOKEN, str(int(CTF_TOKEN) + 1)))))
    return HedgeSDK(client, Mock())


def test_limit_buy_and_forced_exit_keep_explicit_share_quantity():
    sdk = adapter()
    for forced in (False, True):
        data, oid = sdk.prepare("condition", CTF_TOKEN, D("0.16"), D(20), False, forced)
        assert data["taker_amount"] == 20_000_000
        assert data["maker_amount"] == 3_200_000
        assert data["order_type"] == "GTC"
        assert len(oid) == 66
    sdk.client.create_limit_order.assert_called_with(
        token_id=CTF_TOKEN, side="BUY", price="0.16", size="20", post_only=False)


def test_sdk_amount_math_uses_shares_not_usd_across_all_supported_ticks():
    from polymarket._internal.actions.orders.limit import _compute_limit_order_amounts
    for tick, price in [("0.1", "0.2"), ("0.01", "0.16"), ("0.005", "0.165"),
                        ("0.0025", "0.1675"), ("0.001", "0.169"), ("0.0001", "0.1699")]:
        offered, requested = _compute_limit_order_amounts(
            price=D(price), size=D(20), side="BUY", tick_size=D(tick))
        assert requested == 20_000_000
        assert offered == int(D(20) * D(price) * 1_000_000)


def test_order_hash_matches_independent_eip712_encoder_and_neg_risk_domain():
    from eth_account.messages import encode_typed_data
    from eth_utils import keccak
    sdk = adapter()
    order = signed()
    fields = [("salt", "uint256"), ("maker", "address"), ("signer", "address"),
              ("tokenId", "uint256"), ("makerAmount", "uint256"), ("takerAmount", "uint256"),
              ("side", "uint8"), ("signatureType", "uint8"), ("timestamp", "uint256"),
              ("metadata", "bytes32"), ("builder", "bytes32")]
    domain = dict(name="Polymarket CTF Exchange", version="2", chainId=137,
                  verifyingContract=EXCHANGE)
    message = dict(salt=12345, maker=WALLET, signer=WALLET, tokenId=int(CTF_TOKEN),
                   makerAmount=3_200_000, takerAmount=20_000_000, side=0,
                   signatureType=3, timestamp=1_000_000, metadata=ZERO32, builder=ZERO32)
    signable = encode_typed_data(domain_data=domain,
                                message_types={"Order": [dict(name=n, type=t) for n, t in fields]},
                                message_data=message)
    expected = "0x" + keccak(b"\x19" + signable.version + signable.header + signable.body).hex()
    assert sdk.order_id("condition", order) == expected
    sdk.contexts["negative"] = NS(neg_risk=True)
    assert sdk.order_id("negative", order) != expected
    assert sdk.order_id("condition", replace(order, salt=12346)) != expected


def test_persisted_signed_order_round_trip_keeps_identical_wire_request():
    import json
    from polymarket._internal.actions.orders.post import build_post_order_request
    sdk = adapter()
    data, _ = sdk.prepare("condition", CTF_TOKEN, D("0.16"), D(20), False, False)
    recovered = json.loads(json.dumps(data))
    sdk.post(recovered)
    reposted = sdk.client.post_order.call_args.args[0]
    assert build_post_order_request(reposted, owner_api_key="fake-key") == \
        build_post_order_request(signed(), owner_api_key="fake-key")


def test_merge_uses_exact_six_decimal_base_units():
    sdk = adapter()
    sdk.merge("condition", D("12.345678"))
    sdk.client.merge_positions.assert_called_once_with(condition_id="condition", amount=12_345_678)


def test_reject_sdk_size_change_before_submission():
    sdk = adapter()
    sdk.client.create_limit_order.return_value = replace(signed(), taker_amount=21_000_000)
    with pytest.raises(ValueError, match="share quantity"):
        sdk.prepare("condition", CTF_TOKEN, D("0.16"), D(20), False, False)
    sdk.client.post_order.assert_not_called()


@pytest.mark.parametrize("forced", [False, True])
def test_recovery_sell_is_share_exact_and_forced_uses_shares_keyword(forced, monkeypatch):
    refresh = Mock()
    monkeypatch.setattr("polymarket._internal.actions.orders.place._refresh_balance_allowance_sync", refresh)
    sdk = adapter()
    order = replace(signed(), side="SELL", maker_amount=20_000_000, taker_amount=2_800_000,
                    order_type="FOK" if forced else "GTC")
    sdk.client.create_limit_order.return_value = order
    sdk.client.create_market_order = Mock(return_value=order)
    data, _ = sdk.prepare_sell("condition", CTF_TOKEN, D("0.14"), D(20), forced)
    refresh.assert_called_once_with(sdk.client, order)
    assert data["side"] == "SELL" and data["maker_amount"] == 20_000_000
    if forced:
        sdk.client.create_market_order.assert_called_once_with(
            token_id=CTF_TOKEN, side="SELL", shares="20", order_type="FOK")
    else:
        sdk.client.create_limit_order.assert_called_once_with(
            token_id=CTF_TOKEN, side="SELL", price="0.14", size="20")


def test_recovery_sell_balance_selects_protocol_asset_type():
    sdk = adapter()
    sdk.client.get_balance_allowance = Mock(return_value=NS(balance=20_000_000))
    assert sdk.sell_balance("condition", CTF_TOKEN) == D(20)
    sdk.client.get_balance_allowance.assert_called_with(asset_type="CONDITIONAL", token_id=CTF_TOKEN)
    if SDK_VERSION == "0.9.0":
        sdk.contexts["v2"] = NS(protocol="v2")
        sdk.sell_balance("v2", V2_TOKEN)
        sdk.client.get_balance_allowance.assert_called_with(asset_type="CONDITIONAL-V2", token_id=V2_TOKEN)


def test_real_sdk_market_sell_validation_uses_shares_not_amount():
    from polymarket._internal.actions.orders.market import validate_market_order_params
    from polymarket.errors import UserInputError
    params = validate_market_order_params(token_id=CTF_TOKEN, side="SELL", shares="20", order_type="FOK")
    assert params.shares == D(20) and params.order_type == "FOK"
    with pytest.raises(UserInputError):
        validate_market_order_params(token_id=CTF_TOKEN, side="SELL", amount="20", order_type="FOK")


@pytest.mark.parametrize("protocol", ["ctf", "v2"])
def test_recovery_sell_approval_targets_exchange_instead_of_merge_operator(protocol):
    if protocol == "v2" and SDK_VERSION != "0.9.0":
        pytest.skip("V2 requires SDK 0.9.0")
    from eth_abi import encode, decode
    sdk = adapter()
    ctx = sdk.context("condition")
    ctx.protocol, ctx.neg_risk = protocol, True
    token = CTF_TOKEN
    ledger, exchange = LEDGER, NEG_EXCHANGE
    if protocol == "v2":
        token = V2_TOKEN
        ctx.token_ids = (str(int(token) - 1), token)
        ctx.position_erc1155_address = V2_LEDGER
        ledger, exchange = V2_LEDGER, V3_EXCHANGE
    sdk.client._ctx.rpc = NS(eth_call=Mock(return_value="0x" + encode(["bool"], [True]).hex()))
    assert sdk.approved("condition", sell=True, token=token)
    params = sdk.client._ctx.rpc.eth_call.call_args.kwargs
    assert params["to"] == ledger
    assert decode(["address", "address"], bytes.fromhex(params["data"][10:])) == (WALLET, exchange)
    sdk.client.approve_erc1155_for_all = Mock()
    sdk.approve("condition", sell=True, token=token)
    sdk.client.approve_erc1155_for_all.assert_called_once_with(
        token_address=ledger, operator_address=exchange, approved=True)


def test_unknown_sdk_version_fails_before_trading(monkeypatch):
    monkeypatch.setattr("hedge_sdk.version", lambda _: "0.6.0")
    with pytest.raises(RuntimeError, match="installed 0.6.0"):
        adapter()


@pytest.mark.skipif(SDK_VERSION != "0.9.0", reason="Protocol V2 routing added in SDK 0.9.0")
def test_v2_order_hash_matches_sdk_unsigned_order_domain_and_independent_encoder():
    from eth_account.messages import encode_typed_data
    from eth_utils import keccak
    from polymarket._internal.actions.orders.context import resolve_order_exchange_address
    from polymarket._internal.actions.orders.orders import create_unsigned_order, create_signed_order
    from polymarket._internal.actions.orders.types import OrderDraft
    from polymarket._internal.actions.orders.typed_data import build_order_typed_data

    sdk = adapter()
    sdk.contexts["v2"] = NS(protocol="v2", neg_risk=None)
    env = sdk.client._ctx.environment_config
    draft = OrderDraft(chain_id=137, exchange_address=resolve_order_exchange_address(
        env, asset_id=V2_TOKEN, neg_risk=False), expiration=0, funder_address=WALLET,
        offered_amount=3_200_000, order_type="GTC", side="BUY", signer=WALLET,
        requested_amount=20_000_000, asset_id=V2_TOKEN)
    unsigned = create_unsigned_order(draft, wallet=WALLET, wallet_type="EOA")
    signed_order = create_signed_order(unsigned, "0x" + "00" * 65)
    typed_data = build_order_typed_data(unsigned)
    assert typed_data["domain"]["version"] == "3"
    assert typed_data["domain"]["verifyingContract"] == V3_EXCHANGE
    signable = encode_typed_data(full_message=typed_data)
    expected = "0x" + keccak(b"\x19" + signable.version + signable.header + signable.body).hex()
    assert sdk.order_id("v2", signed_order) == expected
    # The legacy domain would generate the wrong durable order ID after a timeout.
    typed_data["domain"]["version"] = "2"
    wrong = encode_typed_data(full_message=typed_data)
    assert expected != "0x" + keccak(b"\x19" + wrong.version + wrong.header + wrong.body).hex()


@pytest.mark.parametrize("protocol", ["ctf", "v2"])
def test_balance_and_approval_select_correct_ledger_and_operator(protocol):
    if protocol == "v2" and SDK_VERSION != "0.9.0":
        pytest.skip("Protocol V2 positions require SDK 0.9.0")
    from eth_abi import encode, decode

    sdk = adapter()
    ctx = sdk.context("condition")
    ctx.protocol = protocol
    if protocol == "v2":
        ctx.token_ids = (str(int(V2_TOKEN) - 1), V2_TOKEN)
        ctx.adapter_address = None
        ctx.position_erc1155_address = V2_LEDGER
    sdk.client._ctx.rpc = NS(eth_call=Mock(return_value="0x" + encode(
        ["uint256[]"], [(20_000_000, 7_000_000)]).hex()))
    assert sdk.balances("condition") == (D(20), D(7))
    params = sdk.client._ctx.rpc.eth_call.call_args.kwargs
    assert params["to"] == ctx.position_erc1155_address
    owners, ids = decode(["address[]", "uint256[]"], bytes.fromhex(params["data"][10:]))
    assert owners == (WALLET, WALLET)
    assert ids == tuple(map(int, ctx.token_ids))

    sdk.client._ctx.rpc.eth_call.return_value = "0x" + encode(["bool"], [True]).hex()
    assert sdk.approved("condition")
    params = sdk.client._ctx.rpc.eth_call.call_args.kwargs
    approval_ledger = V2_LEDGER if protocol == "v2" else LEDGER
    assert params["to"] == approval_ledger
    owner, operator = decode(["address", "address"], bytes.fromhex(params["data"][10:]))
    assert owner == WALLET
    expected = ROUTER if protocol == "v2" else ADAPTER
    assert operator == expected
    sdk.client.approve_erc1155_for_all = Mock()
    sdk.approve("condition")
    sdk.client.approve_erc1155_for_all.assert_called_once_with(
        token_address=approval_ledger, operator_address=expected, approved=True)


def test_negative_risk_approval_uses_ctf_ledger_not_balance_adapter():
    from eth_abi import encode, decode
    from eth_utils import keccak

    sdk = adapter()
    ctx = sdk.context("condition")
    ctx.neg_risk = True
    ctx.position_erc1155_address = NEG_BALANCE_ADAPTER
    ctx.adapter_address = NEG_MERGE_OPERATOR

    def query(*, to, data):
        raw = bytes.fromhex(data[2:])
        if raw[:4] == keccak(b"balanceOfBatch(address[],uint256[])")[:4]:
            assert to == NEG_BALANCE_ADAPTER
            return "0x" + encode(["uint256[]"], [(20_000_000, 20_000_000)]).hex()
        assert raw[:4] == keccak(b"isApprovedForAll(address,address)")[:4]
        if to == NEG_BALANCE_ADAPTER:
            # Reproduced on chain: this adapter forwards balance reads but
            # reverts on the ERC-1155 operator-approval selector.
            raise RuntimeError("JSON-RPC eth_call failed: execution reverted")
        assert to == LEDGER
        assert decode(["address", "address"], raw[4:]) == (WALLET, NEG_MERGE_OPERATOR)
        return "0x" + encode(["bool"], [False]).hex()

    sdk.client._ctx.rpc = NS(eth_call=Mock(side_effect=query))
    assert sdk.balances("condition") == (D(20), D(20))
    assert sdk.approved("condition") is False
    sdk.client.approve_erc1155_for_all = Mock()
    sdk.approve("condition")
    sdk.client.approve_erc1155_for_all.assert_called_once_with(
        token_address=LEDGER, operator_address=NEG_MERGE_OPERATOR, approved=True)


def test_approval_rpc_failure_identifies_read_target_without_submitting():
    sdk = adapter()
    sdk.client._ctx.rpc = NS(eth_call=Mock(side_effect=RuntimeError("execution reverted")))
    sdk.client.approve_erc1155_for_all = Mock()
    with pytest.raises(RuntimeError, match=f"Approval query failed:.*ledger={LEDGER}"):
        sdk.approved("condition")
    sdk.client.approve_erc1155_for_all.assert_not_called()


@pytest.mark.parametrize("protocol", ["ctf", "v2"])
def test_actual_sdk_merge_routes_exact_amount_without_live_rpc(protocol):
    if protocol == "v2" and SDK_VERSION != "0.9.0":
        pytest.skip("Protocol V2 positions require SDK 0.9.0")
    from eth_abi import encode, decode
    from eth_utils import keccak
    from polymarket import SecureClient

    sdk = adapter()
    ctx = sdk.context("condition")
    ctx.protocol = protocol
    ctx.condition_id = "0x" + "12" * (31 if protocol == "v2" else 32)
    if protocol == "v2":
        ctx.token_ids = (str(int(V2_TOKEN) - 1), V2_TOKEN)
        ctx.adapter_address = None
    sdk.client._ctx.wallet = WALLET
    sdk.client._ctx.rpc = NS(eth_call=Mock(return_value="0x" + encode(
        ["uint256[]"], [(20_000_000, 20_000_000)]).hex()))
    sdk.client._dispatch_single_call = Mock()
    sdk.client.merge_positions = lambda **kwargs: SecureClient.merge_positions(sdk.client, **kwargs)
    sdk.merge("condition", D("12.345678"))
    call = sdk.client._dispatch_single_call.call_args.args[0]
    raw = bytes.fromhex(call.data[2:])
    if protocol == "v2":
        assert call.to == ROUTER
        assert raw[:4] == keccak(b"merge(bytes31,uint256)")[:4]
        condition, amount = decode(["bytes31", "uint256"], raw[4:])
    else:
        assert call.to == ADAPTER
        assert raw[:4] == keccak(b"mergePositions(address,bytes32,bytes32,uint256[],uint256)")[:4]
        _, _, condition, partition, amount = decode(
            ["address", "bytes32", "bytes32", "uint256[]", "uint256"], raw[4:])
        assert partition == (1, 2)
    assert condition.hex() == ctx.condition_id[2:]
    assert amount == 12_345_678
