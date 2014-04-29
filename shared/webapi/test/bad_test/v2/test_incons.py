from shared.webapi.validate import ASCIIStringTypeFormat as ASCIIString

def test_method1(param):
    return 42

############## Mapping ##############
map_data = [
    {'NAME': 'incons.test_incons',
     'HTTP_METHOD': 'GET',
     'HTTP_URL': '/test_incons',
     'DESCRIPTION': 'Test',
     'URL_PARAMS': [],
     'QUERY_PARAMS': [['param', ASCIIString(none_is_ok=True),]],
     'FUNCTION': test_method1, },
]
