"""rencode -- Web safe object pickling/unpickling.

The rencode module is a modified version of bencode from the
BitTorrent project.  For complex, heterogeneous data structures with
many small elements, r-encodings take up significantly less space than
b-encodings:

 >>> len(rencode.dumps({'a':0, 'b':[1, 2], 'c':99}))
 13
 >>> len(bencode.bencode({'a':0, 'b':[1, 2], 'c':99}))
 26

The rencode format is not standardized, and may change with different
rencode module versions, so you should check that you are using the
same rencode version throughout your project.
"""

__version__ = '1.0.0-ober'
__all__ = ['dumps', 'loads']

# Original bencode module by Petru Paler, et al.
#
# Modifications by Oleksandr Berchenko:
#
#  - Allow dict keys to be only strings [revert to original bencode].
#  - Lists/tuples are always decoded as lists [revert to original bencode].
#  - Define encode_float function once at the beginning.
#  - Don't check the BooleanType availability (assume we have at least
#    Python 2.3).
#  - Revise exceptions being raised.
#  - Do some refactoring.
#
# Modifications by Connelly Barnes:
#
#  - Added support for floats (sent as 32-bit or 64-bit in network
#    order), bools, None.
#  - [reverted] Allowed dict keys to be of any serializable type.
#  - [reverted] Lists/tuples are always decoded as tuples (thus, tuples can be
#    used as dict keys).
#  - Embedded extra information in the 'typecodes' to save some space.
#  - Added a restriction on integer length, so that malicious hosts
#    cannot pass us large integers which take a long time to decode.
#
# Licensed by Bram Cohen under the "MIT license":
#
#  "Copyright (C) 2001-2002 Bram Cohen
#
#  Permission is hereby granted, free of charge, to any person
#  obtaining a copy of this software and associated documentation files
#  (the "Software"), to deal in the Software without restriction,
#  including without limitation the rights to use, copy, modify, merge,
#  publish, distribute, sublicense, and/or sell copies of the Software,
#  and to permit persons to whom the Software is furnished to do so,
#  subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be
#  included in all copies or substantial portions of the Software.
#
#  The Software is provided "AS IS", without warranty of any kind,
#  express or implied, including but not limited to the warranties of
#  merchantability,  fitness for a particular purpose and
#  noninfringement.  In no event shall the  authors or copyright holders
#  be liable for any claim, damages or other liability, whether in an
#  action of contract, tort or otherwise, arising from, out of or in
#  connection with the Software or the use or other dealings in the
#  Software."
#
# (The rencode module is licensed under the above license as well).
#

import struct

from types import (IntType,
                   LongType,
                   FloatType,
                   StringType,
                   DictType,
                   ListType,
                   TupleType,
                   BooleanType,
                   NoneType)

# Number of bits for serialized floats, either 32 or 64.
FLOAT_BITS = 32

assert FLOAT_BITS in (32, 64)

# Maximum length of integer when written as base 10 string.
MAX_INT_LENGTH = 64

# The bencode 'typecodes' such as i, d, etc.  have been extended and
# relocated on the base-256 character set.
CHR_LIST    = chr(59)
CHR_DICT    = chr(60)
CHR_INT     = chr(61)
CHR_INT1    = chr(62)
CHR_INT2    = chr(63)
CHR_INT4    = chr(64)
CHR_INT8    = chr(65)
CHR_FLOAT32 = chr(66)
CHR_FLOAT64 = chr(44)
CHR_TRUE    = chr(67)
CHR_FALSE   = chr(68)
CHR_NONE    = chr(69)
CHR_TERM    = chr(127)

# Pre-created dictionary for encode_bool function.
BOOL_DICT = {False: CHR_FALSE, True: CHR_TRUE}

# Positive integers with value embedded in typecode.
INT_POS_FIXED_START = 0
INT_POS_FIXED_COUNT = 44

# Dictionaries with length embedded in typecode.
DICT_FIXED_START = 102
DICT_FIXED_COUNT = 25

# Negative integers with value embedded in typecode.
INT_NEG_FIXED_START = 70
INT_NEG_FIXED_COUNT = 32

# Strings with length embedded in typecode.
STR_FIXED_START = 128
STR_FIXED_COUNT = 64

# Lists with length embedded in typecode.
LIST_FIXED_START = STR_FIXED_START + STR_FIXED_COUNT
LIST_FIXED_COUNT = 64

def decode_int(x, f):
    f += 1
    newf = x.index(CHR_TERM, f)
    if newf - f >= MAX_INT_LENGTH:
        raise ValueError('integer overflow')
    try:
        n = int(x[f:newf])
    except (OverflowError, ValueError):
        n = long(x[f:newf])
    return (n, newf + 1)

def decode_intb(x, f):
    f += 1
    return (struct.unpack('!b', x[f:f + 1])[0], f + 1)

def decode_inth(x, f):
    f += 1
    return (struct.unpack('!h', x[f:f + 2])[0], f + 2)

def decode_intl(x, f):
    f += 1
    return (struct.unpack('!l', x[f:f + 4])[0], f + 4)

def decode_intq(x, f):
    f += 1
    return (struct.unpack('!q', x[f:f + 8])[0], f + 8)

def decode_float32(x, f):
    f += 1
    n = struct.unpack('!f', x[f:f + 4])[0]
    return (n, f + 4)

def decode_float64(x, f):
    f += 1
    n = struct.unpack('!d', x[f:f + 8])[0]
    return (n, f + 8)

def decode_string(x, f):
    colon = x.index(':', f)
    try:
        n = int(x[f:colon])
    except (OverflowError, ValueError):
        n = long(x[f:colon])
    colon += 1
    return (x[colon:colon + n], colon + n)

def decode_list(x, f):
    r = []
    f += 1
    while x[f] != CHR_TERM:
        v, f = decode_func[x[f]](x, f)
        r.append(v)
    return (r, f + 1)

def decode_dict(x, f):
    r = {}
    f += 1
    while x[f] != CHR_TERM:
        k, f = decode_func[x[f]](x, f)
        r[k], f = decode_func[x[f]](x, f)
    return (r, f + 1)

def decode_true(x, f):
    return (True, f + 1)

def decode_false(x, f):
    return (False, f + 1)

def decode_none(x, f):
    return (None, f + 1)

decode_func = {}
decode_func['0'] = decode_string
decode_func['1'] = decode_string
decode_func['2'] = decode_string
decode_func['3'] = decode_string
decode_func['4'] = decode_string
decode_func['5'] = decode_string
decode_func['6'] = decode_string
decode_func['7'] = decode_string
decode_func['8'] = decode_string
decode_func['9'] = decode_string
decode_func[CHR_LIST   ] = decode_list
decode_func[CHR_DICT   ] = decode_dict
decode_func[CHR_INT    ] = decode_int
decode_func[CHR_INT1   ] = decode_intb
decode_func[CHR_INT2   ] = decode_inth
decode_func[CHR_INT4   ] = decode_intl
decode_func[CHR_INT8   ] = decode_intq
decode_func[CHR_FLOAT32] = decode_float32
decode_func[CHR_FLOAT64] = decode_float64
decode_func[CHR_TRUE   ] = decode_true
decode_func[CHR_FALSE  ] = decode_false
decode_func[CHR_NONE   ] = decode_none

def make_fixed_length_string_decoders():
    def make_decoder(slen):
        def func(x, f):
            return (x[f + 1:f + 1 + slen], f + 1 + slen)
        return func
    for i in range(STR_FIXED_COUNT):
        decode_func[chr(STR_FIXED_START + i)] = make_decoder(i)

make_fixed_length_string_decoders()

def make_fixed_length_list_decoders():
    def make_decoder(slen):
        def func(x, f):
            r = []
            f += 1
            for _ in range(slen):
                v, f = decode_func[x[f]](x, f)
                r.append(v)
            return (r, f)
        return func
    for i in range(LIST_FIXED_COUNT):
        decode_func[chr(LIST_FIXED_START + i)] = make_decoder(i)

make_fixed_length_list_decoders()

def make_fixed_length_int_decoders():
    def make_decoder(j):
        def func(x, f):
            return (j, f + 1)
        return func
    for i in range(INT_POS_FIXED_COUNT):
        decode_func[chr(INT_POS_FIXED_START + i)] = make_decoder(i)
    for i in range(INT_NEG_FIXED_COUNT):
        decode_func[chr(INT_NEG_FIXED_START + i)] = make_decoder(-1 - i)

make_fixed_length_int_decoders()

def make_fixed_length_dict_decoders():
    def make_decoder(slen):
        def func(x, f):
            r = {}
            f += 1
            for _ in range(slen):
                k, f = decode_func[x[f]](x, f)
                r[k], f = decode_func[x[f]](x, f)
            return (r, f)
        return func
    for i in range(DICT_FIXED_COUNT):
        decode_func[chr(DICT_FIXED_START + i)] = make_decoder(i)

make_fixed_length_dict_decoders()

def loads(x):
    try:
        r, l = decode_func[x[0]](x, 0)
    except (IndexError, KeyError), e:
        raise ValueError(e)
    if l != len(x):
        raise ValueError('decoding failed')
    return r

def encode_int(x, r):
    if 0 <= x < INT_POS_FIXED_COUNT:
        r.append(chr(INT_POS_FIXED_START + x))
    elif -INT_NEG_FIXED_COUNT <= x < 0:
        r.append(chr(INT_NEG_FIXED_START - 1 - x))
    elif -128 <= x < 128:
        r.extend((CHR_INT1, struct.pack('!b', x)))
    elif -32768 <= x < 32768:
        r.extend((CHR_INT2, struct.pack('!h', x)))
    elif -2147483648 <= x < 2147483648:
        r.extend((CHR_INT4, struct.pack('!l', x)))
    elif -9223372036854775808 <= x < 9223372036854775808:
        r.extend((CHR_INT8, struct.pack('!q', x)))
    else:
        s = str(x)
        if len(s) >= MAX_INT_LENGTH:
            raise ValueError('integer overflow')
        r.extend((CHR_INT, s, CHR_TERM))

def encode_float32(x, r):
    r.extend((CHR_FLOAT32, struct.pack('!f', x)))

def encode_float64(x, r):
    r.extend((CHR_FLOAT64, struct.pack('!d', x)))

def encode_bool(x, r):
    r.extend(BOOL_DICT[bool(x)])

def encode_none(x, r):
    r.extend(CHR_NONE)

def encode_string(x, r):
    if len(x) < STR_FIXED_COUNT:
        r.extend((chr(STR_FIXED_START + len(x)), x))
    else:
        r.extend((str(len(x)), ':', x))

def encode_list(x, r):
    if len(x) < LIST_FIXED_COUNT:
        r.append(chr(LIST_FIXED_START + len(x)))
        for i in x:
            encode_func.get(type(i), no_such_type)(i, r)
    else:
        r.append(CHR_LIST)
        for i in x:
            encode_func.get(type(i), no_such_type)(i, r)
        r.append(CHR_TERM)

def encode_dict(x, r):
    if len(x) < DICT_FIXED_COUNT:
        r.append(chr(DICT_FIXED_START + len(x)))
        for k, v in x.items():
            if type(k) != StringType:
                raise ValueError('dict key %s is not a string but %s' %
                                 (k, type(k)))
            encode_string(k, r)
            encode_func.get(type(v), no_such_type)(v, r)
    else:
        r.append(CHR_DICT)
        for k, v in x.items():
            if type(k) != StringType:
                raise ValueError('dict key %s is not a string but %s' %
                                 (k, type(k)))
            encode_string(k, r)
            encode_func.get(type(v), no_such_type)(v, r)
        r.append(CHR_TERM)

encode_func = {}
encode_func[IntType] = encode_int
encode_func[LongType] = encode_int
encode_func[StringType] = encode_string
encode_func[ListType] = encode_list
encode_func[TupleType] = encode_list
encode_func[DictType] = encode_dict
encode_func[BooleanType] = encode_bool
encode_func[NoneType] = encode_none

if FLOAT_BITS == 32:
    encode_func[FloatType] = encode_float32
else:
    encode_func[FloatType] = encode_float64

def no_such_type(x, r):
    raise ValueError('%s type is not supported' % (type(x),))

def dumps(x):
    r = []
    encode_func.get(type(x), no_such_type)(x, r)
    return ''.join(r)

def test():
    f1 = struct.unpack('!f', struct.pack('!f', 25.5))[0]
    f2 = struct.unpack('!f', struct.pack('!f', 29.3))[0]
    f3 = struct.unpack('!f', struct.pack('!f', -0.6))[0]
    L = [[{'a':15, 'bb':f1, 'ccc':f2, '':[f3, [], False, True, '']},
          ['a',
           10 ** 20],
           range(-100000, 100000),
           'b' * 31,
           'b' * 62,
           'b' * 64,
           2 ** 30,
           2 ** 33,
           2 ** 62,
           2 ** 64,
           2 ** 30,
           2 ** 33,
           2 ** 62,
           2 ** 64,
           False,
           False,
           True,
           -1,
           2,
           0]]
    assert loads(dumps(L)) == L
    d = dict(zip([str(a) for a in range(-100000, 100000)],
                  range(-100000, 100000)))
    d.update({'a':20, '20':40, '40':41, str(f1):f2, str(f2):f3, str(f3):False,
              'False':True, 'True':False})
    L = [d, {}, {'5':6}, {'7':7, 'True':8},
         {'9':10, '22':39, '49':50, '44':''}]
    assert loads(dumps(L)) == L
    L = ['', 'a' * 10, 'a' * 100, 'a' * 1000, 'a' * 10000, 'a' * 100000,
         'a' * 1000000, 'a' * 10000000]
    assert loads(dumps(L)) == L
    L = [dict(zip([str(a) for a in range(n)], range(n)))
         for n in range(100)] + ['b']
    assert loads(dumps(L)) == L
    L = [dict(zip([str(a) for a in range(n)], range(-n, 0)))
         for n in range(100)] + ['b']
    assert loads(dumps(L)) == L
    L = [range(n) for n in range(100)] + ['b']
    assert loads(dumps(L)) == L
    L = ['a' * n for n in range(1000)] + ['b']
    assert loads(dumps(L)) == L
    L = ['a' * n for n in range(1000)] + [None, True, None]
    assert loads(dumps(L)) == L
    assert loads(dumps(None)) is None
    assert loads(dumps({'None':None})) == {'None':None}
    assert 1e-10 < abs(loads(dumps(1.1)) - 1.1) < 1e-6

try:
    import psyco
    psyco.bind(dumps)
    psyco.bind(loads)
except ImportError:
    pass

if __name__ == '__main__':
    test()
