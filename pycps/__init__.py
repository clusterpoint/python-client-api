#    Copyright 2012 ClusterPoint, SIA
#
#    This file is part of Pycps.
#
#    Pycps is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    Pycps is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with Pycps.  If not, see <http://www.gnu.org/licenses/>.

import xml.etree.cElementTree as ET

from errors import *
from converters import *
from utils import *
from connection import *
from request import *
from response import *
import query

ET.register_namespace("cps", "www.clusterpoint.com")    # This has global effect.

if __name__ == '__main__':
    Debug._DEBUG = True

# Doctests
    import doctest
    import errors
    import converters
    import utils
    import connection
    import request
    import response
    import query

    def doctest_a_module(module):
        Debug.warn("Running doctests on {0} module ...".format(module.__name__))
        failure_count, test_count = doctest.testmod(module)
        if failure_count:
            Debug.fail("DOCTESTS FAILED!")
            raise Exception()
        else:
            Debug.ok("DOCTESTS PASSED!")

    doctest_a_module(converters)
    doctest_a_module(query)

# Rudimentary functional tests using a CP server running on a local VBox instance.
    import re
    import subprocess

    def test(t):
        try:    # Known bug where first request is rejected
            t.status()
        except APIError:
            pass
        t.clear(type='single', timeout=10)
        t.backup('tmp.tar.gz',backup_type='incremental')
        t.insert({1: {'text': 'aste', 'title': 'lol'}, 2: '<item/>'})
        t.insert(['<document><id>3</id><text>foobar</text></document>','<document><id>4</id></document>'], add_ids=False)
        t.insert({5: '<text>foobar</text>',6: '<text>baz</text>'}, add_ids=True)
        t.insert([{'id':7},{'id':8},{'id':9}], add_ids=False)
        t.replace([{'id':3, 'title': 'one'},{'id':4, 'title': 'two'}], add_ids=False)
        t.replace({3: {'title': 'one prom'},4: {'title': 'two prim'}})
        t.partial_replace({'id':5, 'tabs': 'ss'}, add_ids=False)
        t.update({4: {'text':'lorem'}})
        t.update({34: {'title':'Far fetched.', 'text': 'Something.'}})
        t.delete([3,1])
        t.search(query.terms_from_dict({'title':'lorem'}))
        t.search_delete({'title':'lorem'})
        t.insert({11: {'title':'test','text':'11-test'}, 12: {'title':'test','text':'12-test'},\
                13: {'title':'test','text':'13-test'},14: {'title':'test','text':'14-test'},\
                15:{'title':'test','text':'15-test'}})
        t.search({'title':'test'}, docs=3, offset=1, list={'text':'yes', 'title': 'no'})
        t.retrieve([11,12])
        t.similar(12, 2, 1, docs=2)
        t.lookup([12,15], list={'title': 'no'})
        t.alternatives(query.term('test'), cr=1, idif=1, h=1)
        t.list_words(query.term('test'))
        t.list_last(docs=1, offset=0)
        t.list_first(list={'text':'yes'}, docs=1, offset=0)
        t.reindex()
        t.status()
        #t.restore('tmp.tar.gz',sequence_check=False)
    try:
        vbox_string = subprocess.check_output(["VBoxManage", "guestproperty", "enumerate", "CPS2"])
    except:
        Debug.fail("Can't find VBox for testing!")
    else:
        try:
            index = vbox_string.find("/VirtualBox/GuestInfo/Net/0/V4/I")
            ip_string = re.findall('(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})',vbox_string[index:])[0]
        except:
            Debug.fail("Can't find valid IP for VBox test server!")
        else:
            Debug.warn("Running functests with found VBox server on {0} ...".format(ip_string))
            t=Connection(ip_string, 80, 'test_storage', 'root', 'password')
            test(t)
            Debug.ok("FUNCTESTS PASSED!")
