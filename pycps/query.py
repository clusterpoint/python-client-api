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

import cgi

def term(term, xpath=None, escape=True):
    """ Escapes <, > and & characters in the given term for inclusion into XML (like the search query).
        Also wrap the term in XML tags if xpath is specified.
        Note that this function doesn't escape the @, $, " and other symbols that are meaningful in a search query.

        Args:
            term -- The term text to be escaped (e.g. a search query term).

        Keyword args:
            xpath -- An optional xpath, to be specified if the term is to wraped in tags.
            escape -- An optional parameter - whether to escape the term's XML characters. Default is True.

        Returns:
            Properly escaped xml string for queries.

    >>> term('lorem<4')
    'lorem&lt;4'

    >>> term('3 < bar < 5 $$ True', 'document/foo', False)
    '<document><foo>3 < bar < 5 $$ True</foo></document>'

    >>> term('3 < bar < 5 $$ True', 'document/foo')
    '<document><foo>3 &lt; bar &lt; 5 $$ True</foo></document>'
    """
    prefix = []
    postfix = []
    if xpath:
        tags = xpath.split('/')
        for tag in tags:
            if tag:
                prefix.append('<{0}>'.format(tag))
                postfix.insert(0, '</{0}>'.format(tag))
    if escape:
        term = cgi.escape(term)
    return ''.join(prefix + [term] + postfix)


def terms_from_dict(source):
    """ Convert a dict representing a query to a string.

        Args:
            source -- A dict with query xpaths as keys and text or nested query dicts as values.

        Returns:
            A string composed from the nested query terms given.

    >>> terms_from_dict({'document': {'title': "Title this is", 'text': "A long text."}})
    '<document><text>A long text.</text><title>Title this is</title></document>'

    >>> terms_from_dict({'document/title': "Title this is", 'document/text': "A long text."})
    '<document><title>Title this is</title></document><document><text>A long text.</text></document>'
    """
    parsed = ''
    for xpath, text in source.items():
        if hasattr(text, 'keys'):
            parsed += term(terms_from_dict(text), xpath, escape=False)
        else:
            parsed += term(text, xpath)
    return parsed


def and_terms(*args):
    """ Connect given term strings or list(s) of term strings with an AND operator for querying.

        Args:
            An arbitrary number of either strings or lists of strings representing query terms.

        Returns
            A query string consisting of argument terms and'ed together.
    """
    args = [arg if not isinstance(arg, list) else ' '.join(arg) for arg in args]
    return '({0})'.format(' '.join(args))

def or_terms(*args):
    """ Connect given term strings or list(s) of term strings with a OR operator for querying.

        Args:
            An arbitrary number of either strings or lists of strings representing query terms.

        Returns
            A query string consisting of argument terms or'ed together.
    """
    args = [arg if not isinstance(arg, list) else ' '.join(arg) for arg in args]
    return '{{{0}}}'.format(' '.join(args))

def not_term(term):
    """ Negates a given term string.

        Args:
            A query term string.

        Returns
            A negated argument term string.
    """
    return '~{0}'.format(term)
