from bdq_multi.multi_measures import OccurrenceIdDuplicatesScan


def test_occurrence_id_duplicates():
    m = OccurrenceIdDuplicatesScan()
    m.prepare({})
    rows = [
        {"occurrenceID": "A"},
        {"occurrenceID": "B"},
        {"occurrenceID": "A"},  # dup
        {"occurrenceID": ""},  # empty ignored
    ]
    for r in rows:
        m.consume_row(r)
    out = m.finish()
    assert out.result == 1
    assert out.status == "RUN_HAS_RESULT"

