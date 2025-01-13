from include.helper_functions import get_random_number_from_api

def test_get_random_number_from_api():
    result = get_random_number_from_api(min=1, max=100, count=1)
    assert 1 <= result <= 100

