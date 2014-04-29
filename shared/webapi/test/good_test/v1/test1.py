def test_method1(user_object):
    return 42

############## Mapping ##############
map_data = [
    {'NAME': 'test1.test_method1',
     'HTTP_METHOD': 'GET',
     'HTTP_URL': '/test1',
     'DESCRIPTION': 'Test method',
     'URL_PARAMS': [],
     'QUERY_PARAMS': [],
     'FUNCTION': test_method1, },
]
