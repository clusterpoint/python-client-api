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

from utils import *
from errors import *


def etree_to_dict(source):
    """ Recursively load dict/list representation of an XML tree into an etree representation.

        Args:
            source -- An etree Element or ElementTree.

        Returns:
            A dictionary representing sorce's xml structure where tags with multiple identical childrens
            contain list of all their children dictionaries..

    >>> etree_to_dict(ET.fromstring('<content><id>12</id><title/></content>'))
    {'content': {'id': '12', 'title': None}}

    >>> etree_to_dict(ET.fromstring('<content><list><li>foo</li><li>bar</li></list></content>'))
    {'content': {'list': [{'li': 'foo'}, {'li': 'bar'}]}}
    """
    def etree_to_dict_recursive(parent):
        children = parent.getchildren()
        if children:
            d = {}
            identical_children = False
            for child in children:
                if not identical_children:
                    if child.tag in d:
                        identical_children = True
                        l = [{key: d[key]} for key in d]
                        l.append({child.tag: etree_to_dict_recursive(child)})
                        del d
                    else:
                        d.update({child.tag: etree_to_dict_recursive(child)})
                else:
                    l.append({child.tag: etree_to_dict_recursive(child)})
            return (d if not identical_children else l)
        else:
            return parent.text

    if hasattr(source, 'getroot'):
        source = source.getroot()
    if hasattr(source, 'tag'):
        return {source.tag: etree_to_dict_recursive(source)}
    else:
        raise TypeError("Requires an Element or an ElementTree.")


def dict_to_etree(source, root_tag=None):
    """ Recursively load dict/list representation of an XML tree into an etree representation.

        Args:
            source -- A dictionary representing an XML document where identical children tags are
                    countained in a list.

        Keyword args:
            root_tag -- A parent tag in which to wrap the xml tree. If None, and the source dict
                    contains multiple root items, a list of etree's Elements will be returned.

        Returns:
            An ET.Element which is the root of an XML tree or a list of these.

    >>> dict_to_etree({'foo': 'lorem'}) #doctest: +ELLIPSIS
    <Element 'foo' at 0x...>

    >>> dict_to_etree({'foo': 'lorem', 'bar': 'ipsum'}) #doctest: +ELLIPSIS
    [<Element 'foo' at 0x...>, <Element 'bar' at 0x...>]

    >>> ET.tostring(dict_to_etree({'document': {'item1': 'foo', 'item2': 'bar'}}))
    '<document><item2>bar</item2><item1>foo</item1></document>'

    >>> ET.tostring(dict_to_etree({'foo': 'baz'}, root_tag='document'))
    '<document><foo>baz</foo></document>'

    >>> ET.tostring(dict_to_etree({'title': 'foo', 'list': [{'li':1}, {'li':2}]}, root_tag='document'))
    '<document><list><li>1</li><li>2</li></list><title>foo</title></document>'
    """
    def dict_to_etree_recursive(source, parent):
        if hasattr(source, 'keys'):
            for key, value in source.iteritems():
                sub = ET.SubElement(parent, key)
                dict_to_etree_recursive(value, sub)
        elif isinstance(source, list):
            for element in source:
                dict_to_etree_recursive(element, parent)
        else:   # TODO: Add feature to include xml literals as special objects or a etree subtree
            parent.text = str(source)

    if root_tag is None:
        if len(source) == 1:
            root_tag = source.keys()[0]
            source = source[root_tag]
        else:
            roots = []
            for tag, content in source.iteritems():
                root = ET.Element(tag)
                dict_to_etree_recursive(content, root)
                roots.append(root)
            return roots
    root = ET.Element(root_tag)
    dict_to_etree_recursive(source, root)
    return root


def to_etree(source, root_tag=None):
    """ Convert various representations of an XML structure to a etree Element

        Args:
            source -- The source object to be converted - ET.Element\ElementTree, dict or string.

        Keyword args:
            root_tag -- A optional parent tag in which to wrap the xml tree if no root in dict representation.
                    See dict_to_etree()

        Returns:
            A etree Element matching the source object.

    >>> to_etree("<content/>")  #doctest: +ELLIPSIS
    <Element 'content' at 0x...>

    >>> to_etree({'document': {'title': 'foo', 'list': [{'li':1}, {'li':2}]}})  #doctest: +ELLIPSIS
    <Element 'document' at 0x...>

    >>> to_etree(ET.Element('root'))  #doctest: +ELLIPSIS
    <Element 'root' at 0x...>
    """
    if isinstance(source, ET.ElementTree):
        return source.get_root()
    elif isinstance(source, type(ET.Element(None))):    # cElementTree.Element isn't exposed directly
        return source
    elif isinstance(source, basestring):
        try:
            return ET.fromstring(source)
        except:
            raise XMLError(source)
    elif hasattr(source, 'keys'):   # Dict.
        return dict_to_etree(source, root_tag)
    else:
        raise XMLError(source)


def to_raw_xml(source):
    """ Convert various representations of an XML structure to a normal XML string.

        Args:
            source -- The source object to be converted - ET.Element, dict or string.

        Returns:
            A rew xml string matching the source object.

    >>> to_raw_xml("<content/>")
    '<content/>'

    >>> to_raw_xml({'document': {'title': 'foo', 'list': [{'li':1}, {'li':2}]}})
    '<document><list><li>1</li><li>2</li></list><title>foo</title></document>'

    >>> to_raw_xml(ET.Element('root'))
    '<root />'
    """
    if isinstance(source, basestring):
        return source
    elif hasattr(source, 'getiterator'):    # Element or ElementTree.
        return ET.tostring(source, encoding="utf-8", method="xml")
    elif hasattr(source, 'keys'):   # Dict.
        xml_root = dict_to_etree(source)
        return ET.tostring(xml_root, encoding="utf-8", method="xml")
    else:
        raise TypeError("Accepted representations of a document are string, dict and etree")
