import pytest

from example import square_function


@pytest.mark.parametrize(
    "input_value, expected",
    [
        (0, 0),
        (1, 1),
        (2, 4),
        (-1, 1),
        (-3, 9),
        (10, 100),
    ],
    ids=[
        "zero",
        "one",
        "positive",
        "negative-one",
        "negative",
        "large-positive",
    ],
)
def test_square_function(input_value, expected):
    assert square_function(input_value) == expected
