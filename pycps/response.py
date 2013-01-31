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

# TODO: Raise warning if lxml not used as it is much faster.
try:
    from lxml import etree as ET
except ImportError:
    try:
        # Python 2.5 cET
        import xml.etree.cElementTree as ET
    except ImportError:
        try:
        # Python 2.5 plain ET
            import xml.etree.ElementTree as ET
        except ImportError:
            try:
                # old cET
                import cElementTree as ET
            except ImportError:
                # old ET
                import elementtree.ElementTree as ET

import warnings

from utils import *
from errors import *
from converters import *


def _handle_response(response, command, id_xpath='./id', **kwargs):
    """ Initialize the corect Response object from the response string based on the API command type. """
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
        'status': StatusResponse,
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
        'list-paths': ListPathsResponse,
        'list-facets': ListFacetsResponse}
    try:
        request_class = _response_switch[command]
    except KeyError:
        request_class = Response
    return request_class(response, id_xpath, **kwargs)


class Response(object):
    """ Response object to a request to Clusterpoint Storage.

        Properties:
            seconds -- A float of seconds it took the Clusterpoint Storage to prepare this response.
            storage_name -- The name of Clusterpoint Storage this response was recieved from.
            command -- The command to what this response is made.
    """
    def __init__(self, response, id_xpath='./id', raise_errors=True):
        """
            Args:
                response -- A raw XML response block with envelope and all.

            Keyword args:
                raise_errors -- If True, parse the response looking for errors defined in the API and
                        rise them as exceptions. Default is True.
                id_xpath -- The document id tag xpath relative to the document root used for id extracting.
                        Default is './id'.

            Raises:
                APIError -- Recieved an error in the server response.
                APIWarning -- Recieved an nonfatal error in the server response.
                ResponseError -- Recieved invalid response string.
        """
        Debug.dump('Raw response: \n', response)
        try:
            self._response = ET.fromstring(response)
        except: # Various ET types have differnet errors ..
            raise ResponseError(response)
        if raise_errors:
            self._parse_for_errors()
        self._content = self._response.find('{www.clusterpoint.com}content')
        self._id_xpath = id_xpath.split('/')

    def _parse_for_errors(self):
        """ Look for an error tag and raise APIError for fatal errors or APIWarning for nonfatal ones. """
        error = self._response.find('{www.clusterpoint.com}error')
        if error is not None:
            if error.find('level').text.lower() in ('rejected', 'failed', 'error', 'fatal'):
                raise APIError(error)
            else:
                warnings.warn(APIWarning(error))

    def get_content_dict(self):
        """ Get the Clusterpoint response's content as a dict. See etree_to_dict(). """
        return etree_to_dict(self._content)['{www.clusterpoint.com}content']

    def get_content_etree(self):
        """ Get the Clusterpoint response's content as an etree. """
        return self._content

    def get_content_string(self):
        """ Ge thet Clusterpoint response's content as a string. """
        return ''.join([ET.tostring(element, encoding="utf-8", method="xml")
                        for element in list(self._content)])

    def get_content_field(self, name):
        """ Get the contents of a specific subtag from Clusterpoint Storage's response's content tag.

            Args:
                name -- A name string of the content's subtag to be returned.

            Returns:
                A dict representing the contents of the specified field or a list of dicts
                if there are multiple fields with that tag name. Returns None if no field found.
        """
        fields = self._content.findall(name)
        if not fields:
            return None
        elif len(fields) == 1:
            return etree_to_dict(fields[0])[name]
        else:
            return [etree_to_dict(field)[name] for field in fields]

    @property
    def seconds(self):
        return float(self._response.find('{www.clusterpoint.com}seconds').text)

    @property
    def storage_name(self):
        return self._response.find('{www.clusterpoint.com}storage').text

    @property
    def command(self):
        return self._response.find('{www.clusterpoint.com}command').text


class StatusResponse(Response):
    """ StatusResponse object to a request to Clusterpoint Storage.

        Properties:
            status -- A dict where keys correspond to the Clusterpoint Storage status message structure.
    """
    def __init__(self, *args, **kwargs):
        Response.__init__(self, *args, **kwargs)
        self.status = self.get_content_dict()


class ModifyResponse(Response):
    """ ModifyResponse object to a request to Clusterpoint Storage.

        Properties:
            modified_ids -- A list of ids of documents modified for this response.
    """
    @property
    def modified_ids(self):
        documents = self.get_content_field('document')
        if not isinstance(documents, list):
            documents = [documents]
        return [document['id'] for document in documents]


class SearchDeleteResponse(Response):
    """ SearchResponse object to a request to Clusterpoint Storage.

        Properties:
            hits -- The number of deleted documents.
    """
    @property
    def hits(self):
        return int(self.get_content_field('hits'))


class ListPathsResponse(Response):
    """ ListPathsResponse object to a request to Clusterpoint Storage. """
    def get_paths(self):
        """ Get the list of existing xpaths in the Storage.

            Returns:
                A list with the returned xpath strings.
        """
        return [path.text for path in self._content.find('paths').findall('path')]


class ListResponse(Response):
    """ ListResponse object to a request to Clusterpoint Storage.

        Properties:
            found -- The number of documents returned.
            from_document -- The offset from the begining of the result set.
            to_document -- Offeset pluss document count.
            more -- Number of documents matching but not returned.
            hits -- The number of documents matching the query.
    """
    def _get_doc_list(self):
        # ET.Element returns a list of subelements if pased to the inbuilt list().
        return list(self._content.find('results'))

    def get_documents(self, doc_format='dict'):
        """ Get the documents returned from Storege in this response.

            Keyword args:
                doc_format -- Specifies the doc_format for the returned documents.
                    Can be 'dict', 'etree' or 'string'. Default is 'dict'.

            Returns:
                A dict where keys are document ids and values depending of the required doc_format:
                    A dict representations of documents (see etree_to_dict());
                    A etree Element representing the document;
                    A raw XML document string.

            Raises:
                ParameterError -- The doc_format value is not allowed.
        """
        def get_doc_id(root, rel_path):
            if not rel_path:
                return root.text
            else:
                child = root.find(rel_path[0])
                if child is None:
                    return None
                return get_doc_id(child, rel_path[1:])

        if doc_format == 'dict':
            return dict([(get_doc_id(document, self._id_xpath), etree_to_dict(document)['document']) for
                        document in self._get_doc_list()])
        elif doc_format == 'etree':
            return dict([(get_doc_id(document, self._id_xpath), document) for
                        document in self._get_doc_list()])
        elif doc_format in ('', None, 'string'):
            return dict([(get_doc_id(document, self._id_xpath), ET.tostring(document)) for
                        document in self._get_doc_list()])
        else:
            raise ParameterError("doc_format=" + doc_format)

    @property
    def found(self):
        return int(self.get_content_field('found'))

    @property
    def from_document(self):
        return int(self.get_content_field('from'))

    @property
    def to_document(self):
        return int(self.get_content_field('to'))

    @property
    def more(self):
        # More is in form '=<number>', so drop the '='.
        return int(self.get_content_field('more')[1:])

    @property
    def hits(self):
        return int(self.get_content_field('hits'))


class LookupResponse(ListResponse):
    """ LookupResponse object to a request to Clusterpoint Storage. """
    def _get_doc_list(self):
        # Lookup returns documents in content tag, not in results subtag.
        return self._content.findall('document')


class SearchResponse(ListResponse):
    """ SearchResponse object to a request to Clusterpoint Storage. """
    def get_facets(self):
        """ Get facets from the response.

            Returns:
                A dict in form:
                    {<facet path>: {<term>: <number of hits for this term>
                                    } // Repeated for every term.
                    } // Repeated for every facet.
        """
        return dict([(facet.attrib['path'], dict([(term.text, int(term.attrib['hits']))
                                                  for term in facet.findall('term')]))
                     for facet in self._content.findall('facet')])

    def get_aggregate(self):
        """ Get aggregate data.

            Returns:
                A dict in with queries as keys and results as values.
        """
        return dic([(aggregate.find('query').text, aggregate.find('data').text)
                   for aggregate in self._content.findall('aggregate')])


class WordsResponse(Response):
    """ WordResponse object to a request to Clusterpoint Storage. """
    def get_words(self):
        """ Get words matching the request search terms.

            Returns:
                A dict in form:
                    {<search term>: {<matching word>: <number of times this word is found in the Storage>
                                    } // Repeated for every matching word.
                    } // Repeated for every search term.
        """
        return dict([(word_list.attrib['to'], dict([(word.text, word.attrib['count'])
                                                    for word in word_list.findall('word')]))
                     for word_list in self._content.findall('list')])


class AlternativesResponse(Response):
    """ AlternativesResponse object to a request to Clusterpoint Storage. """
    def get_alternatives(self):
        """ Get the spelling alternatives for search terms.

            Returns:
                A dict in form:
                {<search term>: {'count': <number of times the searh term occurs in the Storage>,
                                 'words': {<an alternative>: {'count': <number of times the alternative occurs in the Storage>,
                                                              'cr': <cr value of the alternative>,
                                                              'idif': <idif value of the alternative>,
                                                              'h': <h value of the alternative>}
                                          } // Repeated for every alternative.
                                }
                } // Repeated for every search term
        """
        return dict([(alternatives.find('to').text,
                      {'count': int(alternatives.find('count').text),
                       'words': dict([(word.text, word.attrib)
                                      for word in alternatives.findall('word')])})
                     for alternatives in
                     self._content.find('alternatives_list').findall('alternatives')])


class ListFacetsResponse(Response):
    """ ListFacetsResponse object to a request to Clusterpoint Storage. """
    def get_facets(self):
        """ Get facets from the response.

            Returns:
                A dict where requested facet paths are keys and a list of coresponding terms are values.
        """
        return dict([(facet.attrib['path'], [term.text
                                             for term in facet.findall('term')])
                     for facet in self._content.findall('facet')])
