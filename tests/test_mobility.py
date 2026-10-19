"""Multi-city Mobility Database schedule integration tests (no network required)."""
import io
import zipfile

from tessera import config
from tessera.mobility import MobilityTransit


def gtfs_zip():
    files = {
        "routes.txt": "route_id,route_short_name,route_long_name,route_type\nR1,21,Central to North,3\n",
        "trips.txt": "route_id,service_id,trip_id,shape_id\nR1,S1,T1,SH1\n",
        "shapes.txt": "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
                      "SH1,13.0800,80.2700,1\nSH1,13.0810,80.2710,2\nSH1,13.0820,80.2720,3\n",
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Central Stop,13.0810,80.2710\n",
    }
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return out.getvalue()


def test_mobility_schedule_parser_returns_routes_shapes_and_stops(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "MOBILITY_CACHE_DIR", str(tmp_path))
    loader = MobilityTransit()
    parsed = loader._parse("chennai", gtfs_zip())
    assert parsed["mode"] == "schedule"
    assert parsed["feed_id"] == config.MOBILITY_FEED_IDS["chennai"]
    assert parsed["route_count"] == 1
    assert parsed["routes"][0]["short"] == "21"
    assert parsed["shapes"][0]["route_id"] == "R1"
    assert len(parsed["shapes"][0]["points"]) == 3
    assert parsed["stops"][0]["name"] == "Central Stop"


def test_mobility_catalog_uses_configured_refresh_token_and_feed_ids(monkeypatch):
    monkeypatch.setattr(config, "MOBILITY_DATABASE_REFRESH_TOKEN", "")
    loader = MobilityTransit()
    url, source = loader._feed_url("hyderabad")
    assert url == config.MOBILITY_FEED_URLS["hyderabad"]
    assert "fallback" in source
    assert set(config.MOBILITY_FEED_IDS) == {"chennai", "hyderabad", "hubli_dharwad"}


def test_india_mode_assigns_three_kafka_partitions_per_city():
    if config.SOURCE_MODE != "india":
        return
    expected = ["bengaluru", "delhi", "chennai", "hyderabad", "hubli_dharwad"]
    assert config.ACTIVE_CITIES == expected
    assert len(config.CITY_PARTITIONS) == 5
    assert sorted(p for ps in config.CITY_PARTITIONS.values() for p in ps) == list(range(15))
    assert config.TOPICS[config.T_RAW][0] == 15
    assert config.TOPICS[config.T_AGG][0] == 15


def test_catalog_prefers_mobility_database_hosted_copy(monkeypatch):
    loader = MobilityTransit()
    loader._token = lambda: "test-access-token"
    seen = {}
    def fake_request(url, headers=None, data=None, timeout=35):
        seen["url"] = url
        assert headers["Authorization"] == "Bearer test-access-token"
        return b'{"latest_dataset":{"hosted_url":"https://files.mobilitydatabase.org/test.zip"},"source_info":{"producer_url":"https://producer.example/feed.zip"}}'
    loader._request = fake_request
    url, source = loader._feed_url("chennai")
    assert url == "https://files.mobilitydatabase.org/test.zip"
    assert source == "Mobility Database hosted dataset"
    assert seen["url"].endswith("/gtfs_feeds/" + config.MOBILITY_FEED_IDS["chennai"])
