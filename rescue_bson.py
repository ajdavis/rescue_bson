# Copyright 2009-2010 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Save a BSON file. This is A. Jesse Jiryu Davis's mad hack of bson/__init__.py
to save as much of a BSON dump file as possible, by catching exceptions at
opportune moments while parsing.
"""

import traceback

import calendar
import datetime
import re
import struct
import warnings

from bson.binary import Binary, OLD_UUID_SUBTYPE
from bson.code import Code
from bson.dbref import DBRef
from bson.errors import (InvalidBSON,
                         InvalidDocument,
                         InvalidStringData)
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.son import SON
from bson.timestamp import Timestamp
from bson.tz_util import utc
import random

try:
    import uuid
    _use_uuid = True
except ImportError:
    _use_uuid = False


# This sort of sucks, but seems to be as good as it gets...
RE_TYPE = type(re.compile(""))

MAX_INT32 = 2147483647
MIN_INT32 = -2147483648
MAX_INT64 = 9223372036854775807
MIN_INT64 = -9223372036854775808

EPOCH_AWARE = datetime.datetime.fromtimestamp(0, utc)
EPOCH_NAIVE = datetime.datetime.utcfromtimestamp(0)


def _get_int(data, position, as_class=None, tz_aware=False, unsigned=False):
    format = unsigned and "I" or "i"
    try:
        value = struct.unpack("<%s" % format, data[position:position + 4])[0]
    except struct.error:
        raise InvalidBSON()
    position += 4
    return value, position


def _get_c_string(data, position, length=None):
    # Make a fake key, append rand int so it doesn't clash
    fake_key = "UNKNOWN" + str(random.randint(0, 99999999))

    if length is None:
        try:
            end = data.index("\x00", position)
        except ValueError:
            # Can't find the end, have to skip this string
            print "Can't decode string at position %d" % position
            raise
    else:
        end = position + length

    try:
        value = unicode(data[position:end], "utf-8")
    except Exception, e:
        print "Can't decode string at positions %d-%d:\n%s" % (
            position, end, data[position:end]
        )
        print
        print
        value = fake_key

    position = end + 1

    return value, position

def _get_number(data, position, as_class, tz_aware):
    num = struct.unpack("<d", data[position:position + 8])[0]
    position += 8
    return num, position


def _get_string(data, position, as_class, tz_aware):
    length = struct.unpack("<i", data[position:position + 4])[0] - 1
    position += 4
    return _get_c_string(data, position, length)


def _get_object(data, position, as_class, tz_aware):
    obj_size = struct.unpack("<i", data[position:position + 4])[0]
    object = _elements_to_dict(data, position + 4, position + obj_size - 1, as_class, tz_aware)
    position += obj_size
#    if "$ref" in object:
#        return (DBRef(object.pop("$ref"), object.pop("$id"),
#                      object.pop("$db", None), object), position)
    return object, position


def _get_array(data, position, as_class, tz_aware):
    obj, position = _get_object(data, position, as_class, tz_aware)
    result = []
    i = 0
    while True:
        try:
            result.append(obj[str(i)])
            i += 1
        except KeyError:
            break
    return result, position


def _get_binary(data, position, as_class, tz_aware):
    length, position = _get_int(data, position)
    subtype = ord(data[position])
    position += 1
    if subtype == 2:
        length2, position = _get_int(data, position)
        if length2 != length - 4:
            raise InvalidBSON("invalid binary (st 2) - lengths don't match!")
        length = length2
    if subtype in (3, 4) and _use_uuid:
        value = uuid.UUID(bytes=data[position:position + length])
        position += length
        return (value, position)
    value = Binary(data[position:position + length], subtype)
    position += length
    return value, position


def _get_oid(data, position, as_class, tz_aware):
    value = ObjectId(data[position:position + 12])
    position += 12
    return value, position


def _get_boolean(data, position, as_class, tz_aware):
    value = data[position] == "\x01"
    position += 1
    return value, position


def _get_date(data, position, as_class, tz_aware):
    seconds = float(struct.unpack("<q", data[position:position + 8])[0]) / 1000.0
    position += 8
    if tz_aware:
        return EPOCH_AWARE + datetime.timedelta(seconds=seconds), position
    return EPOCH_NAIVE + datetime.timedelta(seconds=seconds), position


def _get_code(data, position, as_class, tz_aware):
    code, position = _get_string(data, position, as_class, tz_aware)
    return Code(code), position


def _get_code_w_scope(data, position, as_class, tz_aware):
    _, position = _get_int(data, position)
    code, position = _get_string(data, position, as_class, tz_aware)
    scope, position = _get_object(data, position, as_class, tz_aware)
    return Code(code, scope), position


def _get_null(data, position, as_class, tz_aware):
    return None, position


def _get_regex(data, position, as_class, tz_aware):
    pattern, position = _get_c_string(data, position)
    bson_flags, position = _get_c_string(data, position)
    flags = 0
    if "i" in bson_flags:
        flags |= re.IGNORECASE
    if "l" in bson_flags:
        flags |= re.LOCALE
    if "m" in bson_flags:
        flags |= re.MULTILINE
    if "s" in bson_flags:
        flags |= re.DOTALL
    if "u" in bson_flags:
        flags |= re.UNICODE
    if "x" in bson_flags:
        flags |= re.VERBOSE
    return re.compile(pattern, flags), position


def _get_ref(data, position, as_class, tz_aware):
    position += 4
    collection, position = _get_c_string(data, position)
    oid, position = _get_oid(data, position)
    return DBRef(collection, oid), position


def _get_timestamp(data, position, as_class, tz_aware):
    inc, position = _get_int(data, position, unsigned=True)
    timestamp, position = _get_int(data, position, unsigned=True)
    return Timestamp(timestamp, inc), position


def _get_long(data, position, as_class, tz_aware):
    # Have to cast to long; on 32-bit unpack may return an int.
    value = long(struct.unpack("<q", data[position:position + 8])[0])
    position += 8
    return value, position


_element_getter = {
    "\x01": _get_number,
    "\x02": _get_string,
    "\x03": _get_object,
    "\x04": _get_array,
    "\x05": _get_binary,
    "\x06": _get_null,  # undefined
    "\x07": _get_oid,
    "\x08": _get_boolean,
    "\x09": _get_date,
    "\x0A": _get_null,
    "\x0B": _get_regex,
    "\x0C": _get_ref,
    "\x0D": _get_code,  # code
    "\x0E": _get_string,  # symbol
    "\x0F": _get_code_w_scope,
    "\x10": _get_int,  # number_int
    "\x11": _get_timestamp,
    "\x12": _get_long,
    "\xFF": lambda w, x, y, z: (MinKey(), x),
    "\x7F": lambda w, x, y, z: (MaxKey(), x)}


def _element_to_dict(data, position, as_class, tz_aware):
    element_type = data[position]
    position += 1
    element_name, position = _get_c_string(data, position)
    if element_type not in _element_getter:
        raise Exception("skipping bad element type 0x%x for key %s at position %d" % (
            ord(element_type), repr(element_name), position
        ))

    value, position = _element_getter[element_type](data, position,
                                                    as_class, tz_aware)

    return element_name, value, position


def _elements_to_dict(data, position, end, as_class, tz_aware):
    start = position
    result = as_class()
    while position < end:
        element_start = position
        try:
            (key, value, position) = _element_to_dict(data, position, as_class, tz_aware)
            result[key] = value
        except Exception, e:
            print
            traceback.print_exc()
            print
            print 'skipping sub-document at position %d-%d' % (element_start, end)
            return result
    return result

def decode_all(data, as_class=dict, tz_aware=True):
    """Decode BSON data to multiple documents.

    `data` must be a string of concatenated, valid, BSON-encoded
    documents.

    :Parameters:
      - `data`: BSON data
      - `as_class` (optional): the class to use for the resulting
        documents
      - `tz_aware` (optional): if ``True``, return timezone-aware
        :class:`~datetime.datetime` instances

    .. versionadded:: 1.9
    """
    docs = []
    position = 0
    end = len(data) - 1
    i = 0
    while position < end:

        # Unrecoverable errors:
        obj_size = struct.unpack("<i", data[position:position + 4])[0]
        if len(data) - position < obj_size:
            raise InvalidBSON("objsize too large")
        if data[position + obj_size - 1] != "\x00":
            raise InvalidBSON("bad eoo")


        print '---------------------- %d -------------------' % i
        try:
            print _elements_to_dict(data, position + 4, position + obj_size - 1, as_class, tz_aware)
        except Exception, e:
            print "Discarding document in position %d:\n%s" % (
                position, elements
            )

        position += obj_size
        print
    return docs

if __name__ == '__main__':
    import sys
    f = open(sys.argv[1], 'rb')
    data = f.read()
    docs = decode_all(data)
