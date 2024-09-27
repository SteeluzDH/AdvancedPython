from dataProcess import fetch_data

data = fetch_data

def test_fetch_data():
    assert isinstance(fetch_data(), list)