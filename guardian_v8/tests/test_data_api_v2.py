"""Documented v2 fields, complete cursor snapshots, and exit integration."""
import copy
import json
import time
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch

import pytest
import requests

from data_api import DataApiV2, DataApiError
from guardian import Guardian
from hedge_exit import HedgeExit
from test_hedge_exit import env


WALLET = "0x" + "11" * 20
CONDITION = "0x" + "22" * 32


def row(token="yes", size="20", price=".85", **changes):
    result = dict(proxy_wallet=WALLET, condition_id=CONDITION, token_id=token,
                  current_size=size, total_size="9999", avg_price=price,
                  status="OPEN", title="Example position", mergeable=True)
    result.update(changes)
    return result


def page(rows, cursor=None, more=None):
    return {"data": rows, "pagination": {"has_more": cursor is not None if more is None else more,
                                         "next_cursor": cursor, "offset": 0, "limit": 100}}


def reply(payload=None, status=200, text=None):
    response = requests.Response()
    response.status_code = status
    response.url = "https://data-api.polymarket.com/v2/positions"
    response._content = (json.dumps(payload) if text is None else text).encode()
    return response


def reader(*payloads, wallet=WALLET, **kwargs):
    get = Mock(side_effect=[reply(p) for p in payloads])
    return DataApiV2(wallet, get=get, **kwargs), get


def test_v2_query_and_current_shares_never_use_lifetime_bought_amount():
    api, get = reader(page([row(size=".012345", price=".876543")]))
    position = list(api.list_positions())[0].items[0]
    assert position.size == D(".012345") and position.avg_price == D(".876543")
    assert position.size != D(9999)
    url = get.call_args.args[0]
    params = get.call_args.kwargs["params"]
    assert url == "https://data-api.polymarket.com/v2/positions"
    assert params == dict(user=WALLET, status="OPEN", filter_type="TOKENS", filter_amount="0",
                          sort_by="TOKENS", sort_direction="DESC", limit=100)
    assert get.call_args.kwargs["timeout"] == 10
    assert not {"market", "offset", "sizeThreshold", "size_threshold"}.intersection(params)


def test_json_numbers_are_decoded_as_decimal_without_rounding():
    text = ('{"data":[{"condition_id":"condition","token_id":"123",'
            '"current_size":123456789.123456,"avg_price":0.1234567890123456789}],'
            '"pagination":{"has_more":false,"next_cursor":null}}')
    api = DataApiV2(WALLET, get=Mock(return_value=reply(text=text)))
    position = list(api.list_positions())[0].items[0]
    assert position.size == D("123456789.123456")
    assert position.avg_price == D("0.1234567890123456789")


def test_cursor_walk_preserves_anchors_filters_even_across_empty_and_short_pages():
    api, get = reader(page([row("yes")], "opaque-A"), page([], "opaque-B"), page([row("no")]))
    result = [p for pg in api.list_positions(size_threshold=D(".001"), page_size=100) for p in pg.items]
    assert [p.asset_id for p in result] == ["yes", "no"]
    params = [c.kwargs["params"] for c in get.call_args_list]
    base = params[0]
    assert params[1] == {**base, "cursor": "opaque-A"}
    assert params[2] == {**base, "cursor": "opaque-B"}
    assert "cursor" not in base and "offset" not in params[-1]


def test_zero_threshold_keeps_small_pairs_and_explicit_threshold_maps_to_tokens():
    api, get = reader(page([row("yes", ".000001"), row("no", "0")]))
    assert len(list(api.list_positions())[0].items) == 2
    api, get = reader(page([row("yes", "1.5"), row("no", ".5")]))
    assert [p.asset_id for p in list(api.list_positions(size_threshold=1))[0].items] == ["yes"]
    assert get.call_args.kwargs["params"]["filter_amount"] == "1"


def test_known_empty_wallet_is_a_valid_complete_empty_snapshot():
    api, _ = reader(page([]))
    assert list(api.list_positions())[0].items == ()


@pytest.mark.parametrize("payload", [[], {}, {"error": "retired"}, {"data": None},
                                     {"data": []}, {"data": [], "pagination": {}},
                                     page([], more=True), page([], cursor="X", more=False)])
def test_bad_envelopes_or_incomplete_paging_are_never_empty_positions(payload):
    api, _ = reader(payload)
    with pytest.raises(DataApiError):
        list(api.list_positions())


def test_repeated_cursor_is_an_error_not_a_successful_truncated_snapshot():
    api, _ = reader(page([row("yes")], "A"), page([row("no")], "A"))
    with pytest.raises(DataApiError, match="cursor"):
        list(api.list_positions())


def test_cursor_cycle_and_duplicate_position_rows_are_rejected():
    api, _ = reader(page([], "A"), page([], "B"), page([], "A"))
    with pytest.raises(DataApiError, match="cursor"):
        list(api.list_positions())
    api, _ = reader(page([row()], "A"), page([row()]))
    with pytest.raises(DataApiError, match="repeated a position"):
        list(api.list_positions())


@pytest.mark.parametrize("changes", [dict(current_size=None), dict(current_size="NaN"),
    dict(current_size=-1), dict(current_size=True), dict(avg_price=None), dict(avg_price="Infinity"),
    dict(avg_price="invalid"), dict(avg_price=2), dict(token_id=""), dict(condition_id=None),
    dict(status="CLOSED"), dict(proxy_wallet="another-wallet")])
def test_invalid_row_values_are_not_silently_zero_or_ignored(changes):
    api, _ = reader(page([row(**changes)]))
    with pytest.raises(DataApiError):
        list(api.list_positions())


@pytest.mark.parametrize("status", [400, 410, 429, 500, 503])
def test_http_failures_propagate_for_the_existing_retry_schedule(status):
    get = Mock(return_value=reply({"error": "unavailable"}, status))
    api = DataApiV2(WALLET, get=get)
    with pytest.raises(requests.HTTPError):
        list(api.list_positions())
    assert get.call_count == 1  # no retry storm, writes or v1 fallback


def test_invalid_json_and_transport_errors_propagate():
    api = DataApiV2(WALLET, get=Mock(return_value=reply(text="<html>offline</html>")))
    with pytest.raises(DataApiError, match="JSON"):
        list(api.list_positions())
    api = DataApiV2(WALLET, get=Mock(side_effect=requests.Timeout("read timed out")))
    with pytest.raises(requests.Timeout):
        list(api.list_positions())


def test_v2_base_is_not_prefixed_twice_and_redeemable_rows_are_held_positions():
    api, get = reader(page([row(status="REDEEMABLE")]), base_url="https://example.test/v2/")
    assert list(api.list_positions())[0].items[0].size == 20
    assert get.call_args.args[0] == "https://example.test/v2/positions"


def test_hedge_startup_maps_real_v2_pages_without_old_sdk_positions(env):
    previous, client, sdk = env
    previous.close()
    previous.cfg.hedge_startup_mode = "positions"
    api, get = reader(page([row("yes", proxy_wallet=client.wallet)], "next"),
                      page([row("no", "7", ".15", proxy_wallet=client.wallet)]), wallet=client.wallet)
    client.list_positions = Mock(side_effect=AssertionError("Retired SDK v1 route must not be called"))
    engine = HedgeExit(client, previous.cfg, previous.journal.path, lambda: None, sdk=sdk, positions=api)
    try:
        engine._bootstrap()
        market = engine.state["markets"][CONDITION]
        assert D(market["inventory"]["yes"]["qty"]) == 20
        assert D(market["inventory"]["no"]["qty"]) == 7
        assert D(market["inventory"]["yes"]["cost"]) == 17
        assert sdk.balance_reads == 0 and get.call_count == 2
        with patch("hedge_exit.time.time", return_value=engine.state["since"] + .01):
            engine.step()
        assert sdk.merges == [(CONDITION, D(7))]
        client.list_positions.assert_not_called()
    finally:
        engine.close()


def test_hedge_default_reader_uses_v2_and_configured_host_without_touching_sdk(env):
    previous, client, sdk = env
    previous.close()
    previous.cfg.data_api = "https://example.test"
    engine = HedgeExit(client, previous.cfg, previous.journal.path, lambda: None, sdk=sdk)
    try:
        assert isinstance(engine.positions, DataApiV2)
        assert engine.positions.url == "https://example.test/v2/positions"
        assert engine.positions.wallet == client.wallet
    finally:
        engine.close()


def test_periodic_scan_consumes_v2_current_holdings_and_preserves_original_buy_path(env):
    engine, client, sdk = env
    api, _ = reader(page([row("yes", proxy_wallet=client.wallet),
                         row("no", "12", ".15", proxy_wallet=client.wallet)]), wallet=client.wallet)
    engine.positions = api
    sdk.wallet_balances = (D(20), D(12))
    client.list_positions = Mock(side_effect=AssertionError("Retired route"))
    engine.last_merge_scan = -float("inf")
    engine.step()
    market = engine.state["markets"][CONDITION]
    with patch("hedge_exit.time.time", return_value=market["inventory_since"] + .01):
        engine.step()
    assert sdk.merges == [(CONDITION, D(12))] and not sdk.prepared
    client.list_positions.assert_not_called()


def test_partial_v2_snapshot_failure_does_not_initialize_or_mutate_inventory(env):
    previous, client, sdk = env
    previous.close()
    previous.cfg.hedge_startup_mode = "positions"
    api, _ = reader(page([row(proxy_wallet=client.wallet)], "next"), {"error": "dependency unavailable"},
                    wallet=client.wallet)
    engine = HedgeExit(client, previous.cfg, previous.journal.path, lambda: None, sdk=sdk, positions=api)
    try:
        with pytest.raises(DataApiError):
            engine.step()
        assert not engine.state["initialized"] and not engine.state["markets"]
        assert not engine.ready and not sdk.prepared and not sdk.merges
    finally:
        engine.close()


def test_legacy_sell_gets_every_v2_page_with_the_original_entry_fields():
    guardian = Guardian.__new__(Guardian)
    guardian.cfg = NS(position_threshold=1)
    guardian.position_reader, _ = reader(page([row("yes")], "next"), page([row("no", "7", ".15")]))
    positions = guardian.positions()
    assert len(positions) == 2
    assert positions[0]["asset"] == "yes" and D(positions[0]["avgPrice"]) == D(".85")
    assert D(positions[1]["size"]) == 7


def test_failed_or_partial_sell_snapshot_preserves_holding_timeout_clocks():
    guardian = Guardian.__new__(Guardian)
    guardian.cfg = NS(position_threshold=1)
    guardian.position_reader, _ = reader(page([row()], "next"), {"data": []})
    guardian._holding_since = {"yes": time.time() - 10000}
    before = copy.deepcopy(guardian._holding_since)
    guardian.check_positions()
    assert guardian._holding_since == before
