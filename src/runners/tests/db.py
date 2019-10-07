from runners.helpers import db


def test_db_derive_insert_select():
    tests = [
        {"test": [('A', 'AUTOINCREMENT')], "expected": ""},
        {
            "test": [
                ('A', 'AUTOINCREMENT'),
                ('V', 'VARIANT'),
                ("T", "TIMESTAMP"),
                ("N", "NUMBER"),
            ],
            "expected": "PARSE_JSON(column1),TRY_TO_TIMESTAMP(column2),column3",
        },
    ]
    for test in tests:
        assert test['expected'] == db.derive_insert_select(test['test'])


def test_db_derive_insert_columns():
    tests = [
        {"test": [('A', 'AUTOINCREMENT')], "expected": []},
        {
            "test": [
                ('A', 'AUTOINCREMENT'),
                ('V', 'VARIANT'),
                ("T", "TIMESTAMP"),
                ("N", "NUMBER"),
            ],
            "expected": ["V", "T", "N"],
        },
    ]
    for test in tests:
        actual = db.derive_insert_columns(test['test'])
        assert test['expected'] == list(actual)
