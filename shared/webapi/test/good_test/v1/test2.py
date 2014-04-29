from shared.webapi.validate import ASCIIStringTypeFormat as ASCIIString

def test_method1(user_object, url_int):
    return int(url_int)

def test_method2(user_object, echo_str):
    return echo_str

############## Mapping ##############
map_data = [
    {'NAME': 'test2.test_method1',
     'HTTP_METHOD': 'GET',
     'HTTP_URL': '/test2/test_method1',
     'DESCRIPTION': 'Test method',
     'URL_PARAMS': [['url_int', ASCIIString(none_is_ok=False)]],
     'QUERY_PARAMS': [],
     'FUNCTION': test_method1, },
    {'NAME': 'test2.test_method2',
     'HTTP_METHOD': 'GET',
     'HTTP_URL': '/test2/test_method2',
     'DESCRIPTION': 'Test method',
     'URL_PARAMS': [],
     'QUERY_PARAMS': [['echo_str', ASCIIString(none_is_ok=True), None]],
     'FUNCTION': test_method2, },
]
