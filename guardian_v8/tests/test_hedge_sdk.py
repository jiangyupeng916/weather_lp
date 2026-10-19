"""Verify the pinned SDK's actual amount, hashing and serialization contracts."""
from dataclasses import replace
from decimal import Decimal as D
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


def signed():
    return SignedOrder(builder=ZERO32, expiration=0, maker=WALLET,
                       maker_amount=3_200_000, metadata=ZERO32, order_type="GTC", salt=12345,
                       side="BUY", signature="0x" + "00" * 65, signature_type=3,
                       signer=WALLET, taker_amount=20_000_000, timestamp=1_000_000,
                       token_id="123456789", post_only=False)


def adapter():
    client = NS(create_limit_order=Mock(return_value=signed()),
                merge_positions=Mock(), post_order=Mock(),
                _ctx=NS(environment_config=NS(chain_id=137, standard_exchange=EXCHANGE,
                                              neg_risk_exchange=NEG_EXCHANGE)),
                _resolve_market_position_context=Mock(return_value=NS(neg_risk=False)))
    return HedgeSDK(client, Mock())


def test_limit_buy_and_forced_exit_keep_explicit_share_quantity():
    sdk = adapter()
    for forced in (False, True):
        data, oid = sdk.prepare("condition", "123456789", D("0.16"), D(20), False, forced)
        assert data["taker_amount"] == 20_000_000
        assert data["maker_amount"] == 3_200_000
        assert data["order_type"] == "GTC"
        assert len(oid) == 66
    sdk.client.create_limit_order.assert_called_with(
        token_id="123456789", side="BUY", price="0.16", size="20", post_only=False)


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
    message = dict(salt=12345, maker=WALLET, signer=WALLET, tokenId=123456789,
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
    data, _ = sdk.prepare("condition", "123456789", D("0.16"), D(20), False, False)
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
        sdk.prepare("condition", "123456789", D("0.16"), D(20), False, False)
    sdk.client.post_order.assert_not_called()


def test_unknown_sdk_version_fails_before_trading(monkeypatch):
    monkeypatch.setattr("hedge_sdk.version", lambda _: "0.6.0")
    with pytest.raises(RuntimeError, match="0.5.0"):
        adapter()
