def add(x: int, y: int) -> int:
    return x + y


def test_add():
    inputs = {"x": 3, "y": 4}
    expected = 7
    got = add(**inputs)
    assert got == expected
