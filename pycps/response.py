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
import warnings

from utils import *
from errors import *
from converters import *


def _handle_response(response, command, id_xpath='./id', **kwargs):
    _response_switch = {
        'insert': ModifyResponse,
        'replace': ModifyResponse,
        'partial-replace': ModifyResponse,
        'update': ModifyResponse,
        'delete': ModifyResponse,
        'search-delete': SearchDeleteResponse,
        'reindex': Response,
        'backup': Response,
        'restore': Response,
        'clear': Response,
        'status': Response,
        'search': SearchResponse,
        'retrieve': ListResponse,
        'similar': ListResponse,
        'lookup': LookupResponse,
        'alternatives': AlternativesResponse,
        'list-words': WordsResponse,
        'list-first': ListResponse,
        'list-last': ListResponse,
        'retrieve-last': ListResponse,
        'retrieve-first': ListResponse,
        'show-history': None,
        'list-paths': None}
    try:
        request_class = _response_switch[command]
    except KeyError:
        request_class = Response
    return request_class(response, id_xpath, **kwargs)


class Response(object):
    """ Response object to a request to Clusterpoint Storage.
    """
    def __init__(self, response, id_xpath='./id', raise_errors=True):
        """Convert an XML string response from CPS to a  user selected format (dict/list by default).

            Args:
                response -- A raw XML response block with envelope and all.

            Keyword args:
                raise_errors: If True, parse the response looking for errors defined in api and
                        rise them as exceptions. Default is True.
                id_xpath -- The document id tag xpath relative to root used for id extracting.
                        Default is './id'.

            Returns:
                The procesed xml response content in the specified format.

            Rises:
                APIError: Recieved an error in server response.
        """
        Debug.dump('Raw response: \n', response)
        try:
            self.response = ET.fromstring(response)
        except ET.ParseError:
            raise ResponseError(response)
        if raise_errors:
            self._parse_for_errors()
        self.content = self.response.find('{www.clusterpoint.com}content')
        self._id_xpath = id_xpath.split('/')

    def _parse_for_errors(self):
        error = self.response.find('{www.clusterpoint.com}error')
        if error:
            if error.find('level').text.lower() in ('rejected', 'failed', 'error', 'fatal'):
                raise APIError(error)
            else:
                warnings.warn(APIWarning(error))

    def get_content_dict(self):
        return etree_to_dict(self.content)['{www.clusterpoint.com}content']

    def get_content_etree(self):
        return self.content

    def get_content_string(self):
        return ''.join([ET.tostring(element, encoding="utf-8", method="xml") for element in list(self.content)])

    def get_content_field(self, name):
        fields = self.content.findall(name)
        if not fields:
            return None
        elif len(fields) == 1:
            return etree_to_dict(fields[0])[name]
        else:
            return [etree_to_dict(field)[name] for field in fields]

    @property
    def seconds(self):
        return float(self.response.find('{www.clusterpoint.com}seconds').text)

    @property
    def storage_name(self):
        return self.response.find('{www.clusterpoint.com}storage').text

    @property
    def command(self):
        return self.response.find('{www.clusterpoint.com}command').text


class ModifyResponse(Response):
    @property
    def modified_ids(self):
        documents = self.get_content_field('document')
        if not isinstance(documents, list):
            documents = [documents]
        return [document['id'] for document in documents]


class SearchDeleteResponse(Response):
    @property
    def hits(self):
        return int(self.get_content_field('hits'))


class ListResponse(Response):
    def _get_doc_list(self):
        # ET.Element returns a list of subelements if pased to the inbuilt list().
        return list(self.content.find('results'))
    def get_documents(self, format='dict'):
        def get_doc_id(root, rel_path):
            if not rel_path:
                return root.text
            else:
                child = root.find(rel_path[0])
                if child is None:
                    return None
                return get_doc_id(child, rel_path[1:])

        if format == 'dict':
            return dict([(get_doc_id(document, self._id_xpath), etree_to_dict(document)['document']) for
                        document in self._get_doc_list()])
        elif format == 'etree':
            return self._get_doc_list()
        elif format in ('', None, 'string'):
            return [ET.tostring(document) for document in self._get_doc_list()]
        else:
            raise ParameterError('format=' + format)

    @property
    def found(self):
        return int(self.get_content_field('found'))

    @property
    def from_document(self):
        return int(self.get_content_field('from'))

    @property
    def to_document(self):
        return int(self.get_content_field('to'))


class LookupResponse(ListResponse):
    def _get_doc_list(self):
        # Lookup returns documents in content tag, not in results subtag.
        return self.content.findall('document')


class SearchResponse(ListResponse):
    @property
    def facets(self):
        return self.get_content_field('facets')

    @property
    def aggregate(self):
        return self.get_content_field('agregate')

    @property
    def more(self):
        return self.get_content_field('more')

    @property
    def hits(self):
        return int(self.get_content_field('hits'))


class WordsResponse(Response):
    @property
    def words(self):
        return [{word_list.attrib['to']:
                dict([(word.text, word.attrib['count']) for word in word_list.findall('word')])}
                for word_list in self.content.findall('list')]


class AlternativesResponse(Response):
    @property
    def alternatives(self):
        def build_alternatives(to, count, words):
            to = to.text
            count = int(count.text)
            words = dict([(word.text, word.attrib) for word in words])
            return {to: {'count': count, 'words': words}}

        return [build_alternatives(alternatives.find('to'), alternatives.find('count'),
                                   alternatives.findall('word'))
                for alternatives in self.content.find('alternatives_list').findall('alternatives')]
