from bdq_multi.multi_measures import CoordinateDuplicatesScan


def test_coord_dups_rounding():
    m = CoordinateDuplicatesScan()
    m.prepare({"round_decimals": 2})
    rows = [
        {"decimalLatitude": "-33.9249", "decimalLongitude": "18.4241"},
        {"decimalLatitude": "-33.9250", "decimalLongitude": "18.4242"},
        {"decimalLatitude": "-33.9249", "decimalLongitude": "18.4241"},  # dup
    ]
    for r in rows:
        m.consume_row(r)
    out = m.finish()
    assert out.result >= 1

