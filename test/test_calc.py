from temp_tom.src.calculator import summation, subtraction, multiplication, division


def test_summation():
    """
    Testing Summation function
    """
    assert summation(2, 10) == 12
    assert summation(3, 5) == 8
    assert summation(4, 6) == 10


def test_subtraction():
    """
    Testing Subtraction function
    """
    assert subtraction(8, 2) == 6
    assert subtraction(7, 5) == 2
    assert subtraction(4, 2) == 2


def test_multiplication():
    """
    Testing Multiplication function
    """
    assert multiplication(2, 2) == 4
    assert multiplication(7, 2) == 14
    assert multiplication(10, 2) == 20


def test_division():
    """
    Testing Division function
    """
    assert division(5, 5) == 1
    assert division(70, 10) == 7
    assert division(16, 4) == 4