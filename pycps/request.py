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

from utils import *
from converters import *
import query
from  response import _handle_response


class Request(object):
    """ Handles requests to the Storage."""
    def __init__(self, connection, command, request_id=None, timeout=None, type=None):
        """
            Args:
                connection -- A Connection object to be used for this request.
                command -- The command string for this request.

            Keyword args:
                timeout -- 
                type -- Type of request processing. Can be 'auto', 'single', 'cluster'. Default is 'auto'.
                request_id -- Optional request id identificator.
        """
        self.connection = connection
        self._command = command

# These fields are for the envelope of the request.
        self.request_id = request_id
        self.timestamp = None   # TODO: implement
        self.timeout = timeout
        self.type = type

        self._content = {}  # This holds all fields that will be included in the content subtree.
        self._nested_content = {}   # TODO: Not needed?
        self._documents = []    # List of document strings containing documents.

    def set_documents(self, documents, fully_formed=False):
        """ Wrap documents in the correct root tags, add id fields and convert them to xml strings.

            Args:
                documents -- If fully_formed is False (default), accepts dict where keys are document ids and values can be ether
                            xml string, etree.ElementTree or dict representation of an xml document (see dict_to_etree()).
                            If fully_formed is True, accepts list or single document where ids are integrated in document or
                            not needed and document has the right root tag.

            Keyword args:
                fully_formed  -- If documents are fully formed (contains the right root tags and id fields) set to True
                            to avoid the owerhead of documets beeing parsed at all. If set to True only list of documents or
                            a single document can be pased as 'documents', not a dict of documents. Default is False.
        """
        def add_id(document, id):
            def make_id_tag(root, rel_path, max_depth):
                if max_depth < 0:
                    raise ParameterError("document_id_xpath too deep!")
                if not rel_path:
                    return root
                else:
                    child = root.find(rel_path[0])
                    if child is None:
                        child = ET.Element(rel_path[0])
                        root.append(child)
                    return make_id_tag(child, rel_path[1:], max_depth - 1)
            make_id_tag(document, doc_id_xpath, 10).text = str(id)

        if fully_formed: # documents is a list or single document that contians root tags and id fields. 
            if not isinstance(documents, list):
                documents = [documents]
        else: # documents is dict with ids as keys and documents as values.
            doc_root_tag = self.connection.document_root_xpath  # Local scope is faster.
            doc_id_xpath = self.connection.document_id_xpath.split('/')
            # Convert to etrees.
            documents = dict([(id, to_etree((document if document is not None else
                                             query.term('', doc_root_tag)), doc_root_tag))
                             for id, document in documents.items()])     # TODO: possibly ineficient
            # If root not the same as given xpath, make new root and append to it.
            for id, document in documents.items():
                if document.tag != doc_root_tag:
                    documents[id] = ET.Element(doc_root_tag)
                    documents[id].append(document)  # documents is still the old reference
            # Insert ids in documents and collapse to a list of documents.
            for id, document in documents.items():
                add_id(document, id)
            documents = documents.values()
        self._documents = map(to_raw_xml, documents)

    def set_doc_ids(self, doc_ids):
        """ Build xml documents from a list of document ids.

            Args:
                doc_ids -- A document id or a lost of those.
        """
        if isinstance(doc_ids, list):
            self.set_documents(dict.fromkeys(doc_ids))
        else:
            self.set_documents({doc_ids: None})

    def add_property(self, set_property, name, starting_value, tag_name=None):
        """ Set properies of atributes stored in content using stored common fdel and fget and given fset.

            Args:
                set_property -- Function that sets given property.
                name -- Name of the atribute this property must simulate. Used as key in content dict by default.
                starting_value -- Starting value of given property.

            Keyword args:
                tag_name -- The tag name stored in conted dict as a key if different to name.
        """
        def del_property(self, tag_name):
            try:
                del self._content[tag_name]
            except KeyError:
                pass

        def get_property(self, tag_name):
            try:
                return self._content[tag_name]
            except KeyError:
                return None

        tag_name = (name if tag_name is None else tag_name)
        fget = lambda self: get_property(self, tag_name)
        fdel = lambda self: del_property(self, tag_name)
        fset = lambda self, value: set_property(value)
        setattr(self.__class__, name, property(fget, fset, fdel))
        set_property(starting_value)

# Setters for all self._content stored fields handling special processing as needed.
# TODO: Make add_property() generate the simple setters?
#       But maybe it's convieniet to have explicit ones for creating custum Request form base Request.
    def set_backup_file(self, value):
        if value.lower().endswith('.tar.gz'):    # Not checked by server.
            self._content['file'] = value
        else:
            raise ValueError("Invalid backup_file path. Filename must end with '.tar.gz'.)")

    def set_backup_type(self, value):
        if value is not None:
            self._content['type'] = value

    def set_sequence_check(self, value):
        if value is not None:
            self._content['type'] = ('yes' if value else 'no')

    def set_query(self, value):
        """ Convert a dict form of query in a string of needed and store the query string.

            Args:
                value -- A query string or a dict with query xpaths as keys and text or
                        nested query dicts as values.
        """
        if isinstance(value, basestring) or value is None:
            self._content['query'] = value
        elif hasattr(value, 'keys'):
            self._content['query'] = query.terms_from_dict(value)
        else:
            raise TypeError("Query must be a string or dict. Got: " + type(value) + " insted!")

    def set_docs(self, value):
        if value is not None:
            self._content['docs'] = str(value)

    def set_offset(self, value):
        if value is not None:
            self._content['offset'] = str(value)

    def set_list(self, value):
        if value is not None:
            self._content['list'] = '\n'.join([query.term(value, key) for (key, value) in value.items()])

    def set_ordering(self, value):
        if value is not None:
            if isinstance(value, basestring):
                self._content['ordering'] = value
            else:
                self._content['ordering'] = '\n'.join(value)

    def set_agregate(self, value):
        if value is not None:
            self._content['agregate'] = value

    def set_facet(self, value):
        if value is not None:
            self._content['facet'] = value

    def set_facet_size(self, value):
        if value is not None:
            self._content['facet_size'] = str(value)

    def set_stem_lang(self, value):
        if value is not None:
            self._content['stem_lang'] = value

    def set_exact_match(self, value):
        if value is not None:
            self._content['exact_match'] = value

    def set_group(self, value):
        if value is not None:
            self._content['group'] = value

    def set_group_size(self, value):
        if value is not None:
            self._content['group_size'] = str(value)

    def set_cr(self, value):
        if value is not None:
            self._content['cr'] = str(value)

    def set_idif(self, value):
        if value is not None:
            self._content['idif'] = str(value)

    def set_h(self, value):
        if value is not None:
            self._content['h'] = str(value)

    def set_docid(self, value):
        if value is not None:
            self._content['id'] = str(value)

    def set_text(self, value):
        if value is not None:
            self._content['text'] = value

    def set_len(self, value):
        if value is not None:
            self._content['len'] = str(value)

    def set_quota(self, value):
        if value is not None:
            self._content['quota'] = str(value)

    def set_path(self, value):
        if value is not None:
            self._content['path'] = value

    def get_xml_request(self):
        """ Make xml request string from stored request information.

            Returns:
                A properly formated XMl request string containing all set request fields and
                wraped in connections envelope.
        """
        def wrap_xml_content(xml_content):
            """ Wrap XML content string in the correct CPS request envelope."""
            fields = ['<?xml version="1.0" encoding="utf-8"?>\n',
                      '<cps:request xmlns:cps="www.clusterpoint.com">\n',
                      '<cps:storage>', self.connection._storage, '</cps:storage>\n']
            if self.timestamp:
                fields += []    # TODO: implement
            if self.request_id:
                fields += ['<cps:request_id>', str(self.request_id), '</cps:request_id>\n']
            if self.connection.reply_charset:
                fields += []    # TODO: implement
            if self.connection.application:
                fields += ['<cps:application>', self.connection.application, '</cps:application>\n']
            fields += ['<cps:command>', self._command, '</cps:command>\n',
                       '<cps:user>', self.connection._user, '</cps:user>\n',
                       '<cps:password>', self.connection._password, '</cps:password>\n']
            if self.timeout:
                fields += ['<cps:timeout>', str(self.timeout), '</cps:timeout>\n']
            if self.type:
                fields += ['<cps:type>', self.type, '</cps:type>\n']
            if xml_content:
                fields += ['<cps:content>\n', xml_content, '\n</cps:content>\n']
            else:
                fields += '<cps:content/>\n'
            fields += '</cps:request>\n'
            # String concat from list faster than incremental concat.
            xml_request = ''.join(fields)
            return xml_request

        xml_content = []
        if self._documents:
            xml_content += self._documents
        for key, value in self._nested_content.items():
            if value:
                xml_content += ['<{0}>'.format(key)] +\
                    ['<{0}>{1}</{0}>'.format(sub_key, sub_value) for sub_key, sub_value in value if sub_value] +\
                    ['</{0}>'.format(key)]
        for key, value in self._content.items():
            if not isinstance(value, list):
                value = [value]
            xml_content += ['<{0}>{1}</{0}>'.format(key, item) for item in value if item]
        xml_content = '\n'.join(xml_content)
        return wrap_xml_content(xml_content)

    def send(self):
        """ Send an XML string version of content through the connection.

        Returns:
            Response object.
        """
        xml_request = self.get_xml_request()
        Debug.warn('-' * 25)
        Debug.warn(self._command)
        Debug.dump("doc: \n", self._documents)
        Debug.dump("cont: \n", self._content)
        Debug.dump("nest cont \n", self._nested_content)
        Debug.dump("Request: \n", xml_request)
        return _handle_response(self.connection._send_request(xml_request),
                                         self._command, self.connection.document_id_xpath)


class BackupRequest(Request):
    def __init__(self, connection, backup_file, backup_type=None, **kwargs):
        """
            Args:
                backup_file --  String with full path of the backup archive to be created.
                                File name must end with '.tar.gz'.
                See Request.__init__().

            Keyword args:
                backup_type --  Backup type string, can be ether 'full' or 'incremental'. Default is 'incremental' if posible.
                See Request.__init__().
        """
        Request.__init__(self, connection, 'backup', **kwargs)
        self.add_property(self.set_backup_file, 'backup_file', backup_file, 'file')
        self.add_property(self.set_backup_type, 'backup_type', backup_type, 'type')


class RestoreRequest(Request):
    def __init__(self, connection, backup_file, sequence_check=None, **kwargs):
        """
            Args:
                backup_file --  String with full path of the backup archive to be created.
                                File name must end with '.tar.gz'.
                See Request.__init__().

            Keyword args:
                sequence_check --  Ccheck for valid incremental backup sequence if True.
                        Default is True.
                See Request.__init__().
        """
        Request.__init__(self, connection, 'restore', **kwargs)
        self.add_property(self.set_backup_file, 'backup_file', backup_file, 'file')
        self.add_property(self.set_sequence_check, 'sequence_check', sequence_check)


class ModifyRequest(Request):
    """ Base request for insert, update, replace and partial_replace command requests."""
    def __init__(self, connection, documents, fully_formed=False, **kwargs):
        """
            Args:
                documents -- If fully_formed is False (default), accepts dict where keys are document ids and values can be ether
                            xml string, etree.ElementTree or dict representation of an xml document (see dict_to_etree()).
                            If fully_formed is True, accepts list or single document where ids are integrated in document or
                            not needed and document has the right root tag.

            Keyword args:
                fully_formed  -- If documents are fully formed (contains the right root tags and id fields) set to True
                            to avoid the owerhead of documets beeing parsed at all. If set to True only list of documents or
                            a single document can be pased as 'documents', not a dict of documents. Default is False.

                See Request.__init__().
        """
        Request.__init__(self, connection, None, **kwargs)
        self.set_documents(documents, fully_formed)


class InsertRequest(ModifyRequest):
    def __init__(self, *args, **kwargs):
        """
            Args:
                documents -- If fully_formed is False (default), accepts dict where keys are document ids and values can be ether
                            xml string, etree.ElementTree or dict representation of an xml document (see dict_to_etree()).
                            If fully_formed is True, accepts list or single document where ids are integrated in document or
                            not needed and document has the right root tag.

            Keyword args:
                fully_formed  -- If documents are fully formed (contains the right root tags and id fields) set to True
                            to avoid the owerhead of documets beeing parsed at all. If set to True only list of documents or
                            a single document can be pased as 'documents', not a dict of documents. Default is False.

                See Request.__init__().
        """
        ModifyRequest.__init__(self, *args, **kwargs)
        self._command = 'insert'


class UpdateRequest(ModifyRequest):
    def __init__(self, *args, **kwargs):
        """
            Args:
                documents -- If fully_formed is False (default), accepts dict where keys are document ids and values can be ether
                            xml string, etree.ElementTree or dict representation of an xml document (see dict_to_etree()).
                            If fully_formed is True, accepts list or single document where ids are integrated in document or
                            not needed and document has the right root tag.

            Keyword args:
                fully_formed  -- If documents are fully formed (contains the right root tags and id fields) set to True
                            to avoid the owerhead of documets beeing parsed at all. If set to True only list of documents or
                            a single document can be pased as 'documents', not a dict of documents. Default is False.

                See Request.__init__().
        """
        ModifyRequest.__init__(self, *args, **kwargs)
        self._command = 'update'


class ReplaceRequest(ModifyRequest):
    def __init__(self, *args, **kwargs):
        """
            Args:
                documents -- If fully_formed is False (default), accepts dict where keys are document ids and values can be ether
                            xml string, etree.ElementTree or dict representation of an xml document (see dict_to_etree()).
                            If fully_formed is True, accepts list or single document where ids are integrated in document or
                            not needed and document has the right root tag.

            Keyword args:
                fully_formed  -- If documents are fully formed (contains the right root tags and id fields) set to True
                            to avoid the owerhead of documets beeing parsed at all. If set to True only list of documents or
                            a single document can be pased as 'documents', not a dict of documents. Default is False.

                See Request.__init__().
        """
        ModifyRequest.__init__(self, *args, **kwargs)
        self._command = 'replace'


class PartialReplaceRequest(ModifyRequest):
    def __init__(self, *args, **kwargs):
        """
            Args:
                documents -- If fully_formed is False (default), accepts dict where keys are document ids and values can be ether
                            xml string, etree.ElementTree or dict representation of an xml document (see dict_to_etree()).
                            If fully_formed is True, accepts list or single document where ids are integrated in document or
                            not needed and document has the right root tag.

            Keyword args:
                fully_formed  -- If documents are fully formed (contains the right root tags and id fields) set to True
                            to avoid the owerhead of documets beeing parsed at all. If set to True only list of documents or
                            a single document can be pased as 'documents', not a dict of documents. Default is False.

                See Request.__init__().
        """
        ModifyRequest.__init__(self, *args, **kwargs)
        self._command = 'partial-replace'


class SearchRequest(Request):
    def __init__(self, connection, query, docs=None, offset=None, list=None, ordering=None, agregate=None,
                 facet=None, facet_size=None, stem_lang=None, exact_match=None, group=None, group_size=None, **kwargs):
        """
            Args:
                query -- A query string where all <, > and & characters that aren't supposed to be XML
                        tags, should be escaped or a dict where keys are query xpaths and values ether
                        query texts or nested dicts.
                        (see term()).
                See Request.__init__().

            Keyword args:
                docs -- Number of documents to be returned. Default is 10.
                offset -- Offset from the beginning of the result set. Default is 0.
                list -- Defines which tags of the search results should be listed in the response.
                        A dict with tag xpaths as keys and listing option strings ('yes', 'no', 'snippet', 'highlight') as values.
                facet -- A string of paths for facets or a list of those.
                facet_size -- Maximum number of returned facet value count. Default is 0 - returns all values .
                ordering --  Defines the order in which results should be returned. Either a string or a list of those. See helper functions for building these.
                agregate -- Defines aggregation queries for the search request.
                        Ether a single aggregation query string, or a list of strings.
                stem_lang -- Stemming language idetificator string. Available are:
                        'lv', 'en', 'ru', 'fr', 'es', 'pt', 'it', 'ro', 'de', 'nl',
                        'sv', 'no', 'da', 'fi', 'hu', 'tr'. Defalt is None.
                exact_match -- Exact match option strinig. Available are:
                        'text', 'binary', 'all', None. Default is None.
                group -- Tag name of tag for which groups were created.
                group_size -- Maximum number of documents returned from one group. Default id 0 (no grouping performed).
                See Request.__init__().
        """
        Request.__init__(self, connection, 'search', **kwargs)
        self.add_property(self.set_query, 'query', query)
        self.add_property(self.set_docs, 'docs', docs)
        self.add_property(self.set_offset, 'offset', offset)
        self.add_property(self.set_list, 'list', list)
        self.add_property(self.set_ordering, 'odering', ordering)   # TODO: helper method
        self.add_property(self.set_agregate, 'agregate', agregate)  # TODO: Check details
        self.add_property(self.set_facet, 'facet', facet)
        self.add_property(self.set_facet_size, 'facet_size', facet_size)
        self.add_property(self.set_stem_lang, 'stem_lang', stem_lang)
        self.add_property(self.set_exact_match, 'exact_match', exact_match)
        self.add_property(self.set_group, 'group', group)
        self.add_property(self.set_group_size, 'group_size', group_size)


class ListWordsRequest(SearchRequest):
    def __init__(self, *args, **kwargs):
        """
            Args:
                query -- A query string where all <, > and & characters that aren't supposed to be XML
                        tags, should be escaped or a dict where keys are query xpaths and values ether
                        query texts or nested dicts.
                        (see term()).
                See Request.__init__().

            Keyword args:
                See Request.__init__().
        """
        SearchRequest.__init__(self, *args, **kwargs)
        self._command = 'list-words'


class SearchDeleteRequest(SearchRequest):
    def __init__(self, *args, **kwargs):
        """
            Args:
                See SearchRequest
                See Request.__init__().

            Keyword args:
                See Request.__init__().
        """
        SearchRequest.__init__(self, *args, **kwargs)
        self._command = 'search-delete'


class LookupRequest(Request):
    def __init__(self, connection, doc_ids, list=None, **kwargs):
        """
            Args:
                doc_ids -- Single document id or a list of them.
                See Request.__init__()

            Keyword args:
                list -- Defines which tags of the search results should be listed in the response.
                        A dict with tag xpaths as keys and listing option strings ('yes', 'no', 'snippet', 'highlight') as values.
                See Request.__init__()
        """
        Request.__init__(self, connection, 'lookup', **kwargs)
        self.add_property(self.set_list, 'list', list)
        self.set_doc_ids(doc_ids)


class RetrieveRequest(Request):
    def __init__(self, connection, doc_ids, **kwargs):
        """
            Args:
                doc_ids -- Single document id or a list of them.
                See Request.__init__().

            Keyword args:
                See Request.__init__()
        """
        Request.__init__(self, connection, 'retrieve', **kwargs)
        self.set_doc_ids(doc_ids)


class DeleteRequest(RetrieveRequest):
    def __init__(self, *args, **kwargs):
        """
            Args:
                doc_ids -- Single document id or a list of them.
                See Request.__init__()

            Keyword args:
                See Request.__init__()
        """
        RetrieveRequest.__init__(self, *args, **kwargs)
        self._command = 'delete'


class AlternativesRequest(Request):
    def __init__(self, connection, query, cr=None, idif=None, h=None, **kwargs):
        """
            Args:
                query -- A query string where all <, > and & characters that aren't supposed to be XML
                        tags, should be escaped or a dict where keys are query xpaths and values ether
                        query texts or nested dicts.
                        (see term()).
                See Request.__init__().

            Keyword args:
                cr -- Minimum ratio between the occurrence of the alternative and the occurrence of the search term.
                    If this parameter is increased, less results are returned while performance is improved. Default is 2.0.
                idif -- A number that limits how much the alternative may differ from the search term,
                    the greater the idif value, the greater the allowed difference.
                    If this parameter is increased, more results are returned while performance is decreased. Default is 3.0.
                h --  A number that limits the overall estimate of the quality of the alternative,
                    the greater the cr value and the smaller the idif value, the greater the h value.
                    If this parameter is increased, less results are returned while performance is improved. Default is 2.5.
                See Request.__init__()
        """
        Request.__init__(self, connection, 'alternatives', **kwargs)
        self.add_property(self.set_query, 'query', query)
        self.add_property(self.set_cr, 'cr', cr)
        self.add_property(self.set_idif, 'idif', idif)
        self.add_property(self.set_h, 'h', h)


class LastFirstRequest(Request):
    """ Base class for first/last retrieve/list commands."""
    def __init__(self, connection, list=None, docs=None, offset=None, **kwargs):
        """
            Args:
                See Request.__init__().

            Keyword args:
                list -- Defines which tags of the search results should be listed in the response.
                        A dict with tag xpaths as keys and listing option strings ('yes', 'no',
                        'snippet', 'highlight') as values.
                docs -- Number of documents to be returned. Default is 10.
                offset -- Offset from the beginning of the result set. Default is 0.
                See Request.__init__()
        """
        Request.__init__(self, connection, None, **kwargs)
        self.add_property(self.set_docs, 'docs', docs)
        self.add_property(self.set_offset, 'offset', offset)
        self.add_property(self.set_list, 'list', list)


class ListLastRequest(LastFirstRequest):
    def __init__(self, *args, **kwargs):
        """
            Args:
                See Request.__init__().

            Keyword args:
                list -- Defines which tags of the search results should be listed in the response.
                        A dict with tag xpaths as keys and listing option strings ('yes', 'no',
                        'snippet', 'highlight') as values.
                docs -- Number of documents to be returned. Default is 10.
                offset -- Offset from the beginning of the result set. Default is 0.
                See Request.__init__()
        """
        LastFirstRequest.__init__(self, *args, **kwargs)
        self._command = 'list-last'


class ListFirstRequest(LastFirstRequest):
    def __init__(self, *args, **kwargs):
        """
            Args:
                See Request.__init__().

            Keyword args:
                list -- Defines which tags of the search results should be listed in the response.
                        A dict with tag xpaths as keys and listing option strings ('yes', 'no',
                        'snippet', 'highlight') as values.
                docs -- Number of documents to be returned. Default is 10.
                offset -- Offset from the beginning of the result set. Default is 0.
                See Request.__init__()
        """
        LastFirstRequest.__init__(self, *args, **kwargs)
        self._command = 'list-first'


class RetrieveLastRequest(LastFirstRequest):
    def __init__(self, *args, **kwargs):
        """
            Args:
                See Request.__init__().

            Keyword args:
                docs -- Number of documents to be returned. Default is 10.
                offset -- Offset from the beginning of the result set. Default is 0.
                See Request.__init__()
        """
        LastFirstRequest.__init__(self, *args, **kwargs)
        self._command = 'retrieve-last'


class RetrieveFirstRequest(LastFirstRequest):
    def __init__(self, *args, **kwargs):
        """
            Args:
                See Request.__init__().

            Keyword args:
                docs -- Number of documents to be returned. Default is 10.
                offset -- Offset from the beginning of the result set. Default is 0.
                See Request.__init__()
        """
        LastFirstRequest.__init__(self, *args, **kwargs)
        self._command = 'retrieve-first'


class SimilarRequest(Request):
    def __init__(self, connection, source, len, quota, mode='id', offset=0, docs=None, query=None, **kwargs):
        """
        Args:
            source -- A ID of the source document - the one that You want to search similar documents to
                    OR a text (selected based on mode value)
            len -- Number of keywords to extract from the source.
            quota -- Minimum number of keywords matching in the destination.
            See Request.__init__().

        Keyword args:
            mode -- If is 'id', source is interpreted as a document id, if is 'text', source is interpreted as text.
            offset -- Number of results to skip before returning the following ones.
            docs -- Number of documents to retrieve. Default is 10.
            query -- An optional query that all found documents have to match against. See SearchRequest.
            See Request.__init__()
        """
        Request.__init__(self, connection, 'similar', **kwargs)
        if mode == 'id':
            self.add_property(self.set_docid, 'docid', source, 'id')
        else:
            self.add_property(self.set_text, 'text', source)
        self.add_property(self.set_len, 'len', len)
        self.add_property(self.set_quota, 'quota', quota)
        self.add_property(self.set_offset, 'offset', offset)
        self.add_property(self.set_docs, 'docs', docs)
        self.add_property(self.set_query, 'query', query)


class ListFacetsRequest(Request):
    def __init__(self, connection, paths, **kwargs):
        """
        Args:
            paths -- A single facet path string or a list of them.
            See Request.__init__().

        Keyword args:
            See Request.__init__()
        """
        Request.__init__(self, connection, 'list-facets', **kwargs)
        self.add_property(self.set_path, 'paths', paths, 'path')
