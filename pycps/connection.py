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

import httplib
import urlparse
import socket

from request import *


class Connection(object):
    """ Connection object class to hold connections with CPS and use them.

    .. note:: For now only http protocol suported!

    """

    def __init__(self, url, storage, user, password,
                    document_root_xpath = 'document', document_id_xpath = './id',
                    selector_url = '/cgi-bin/cps2-cgi', application='PYCPS',  reply_charset=None):
        """ Create a new connection to CPS.

            Args:
                url -- The connection string containing host, port and scheme of connection.
                        Example: 'tcp://127.0.0.1:5550'.
                storage -- A CPS storage name string.
                user -- A user name string.
                password -- A user password string.

            Keyword args:
                document_root_tag -- A custum document root tag. Default is 'document'.
                document_id_tag -- A custum document id xpath relative document root. Default is './id'.
                selector_urli -- A nonstandart selector url for xml requests. Default is '/cgi-bin/cps2-cgi'.
                application -- Optional application string. Default is 'PYCPS'.
                reply_charset -- Optional reply charset string.
        """
        self._storage = storage
        self._user = user
        self._password = password
        self._selector_url = selector_url

        self.document_root_xpath = document_root_xpath
        self.document_id_xpath = document_id_xpath
        self.application = application
        self.reply_charset = reply_charset

        self._set_url(url)
        self._open_connection()

    def _set_url(self, url):
        if url in ('unix', 'unix://', '', None):
            self._scheme = 'unix'
            self._path = 'unix:///usr/local/cps2/storages/{0}/storage.sock'.\
                        format(self._storage.replace('/', '_'))
            self._port = 0
        else:
            url = urlparse.urlparse(url)
            if url.scheme.lower() == 'http':
                self._scheme = 'http'
                self._host = url.hostname
                self._port = url.port if url.port else 80
            elif url.scheme.lower() == 'unix':
                self._scheme = 'unix'
                self._path = url.path
                self._port = 0
            elif url.scheme.lower() == 'tcp':
                self._scheme = 'tcp'
                self._host = url.hostname
                self._port = url.port if url.port else 5550
            else:
                raise ParameterError(dump=url)

    def _open_connection(self):
        """ Open a new connection socket to the CPS."""
        if self._scheme == 'unix':
            self._connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
            self._connection.connect(self._path)
        elif self._scheme == 'tcp':
            self._connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.SOL_TCP)
            self._connection.connect((self._host, self._port))
        elif self._scheme == 'http':
            self._connection =  httplib.HTTPConnection(self._host, self._port, strict=False)
        else:
            raise ConnectionError("Connection scheme not recognized!")


    def _send_request(self, xml_request):
        """ Send the prepared XML request block to the CPS using the corect protocol.

            Args:
                xml_request -- A fully formed xml request string for the CPS.

            Returns:
                The raw xml response string.

            Raises:
                ConnectionError -- Can't establish a connection with the server.
        """
        if self._scheme == 'http':
            return self._send_http_request(xml_request)
        else:
            return self._send_socket_request(xml_request)

    def _send_http_request(self, xml_request):
        """ Send a request via HTTP protocol.

            Args:
                xml_request -- A fully formed xml request string for the CPS.

            Returns:
                The raw xml response string.
        """
        headers = {"Host": self._host, "Content-Type": "text/xml", "Recipient": self._storage}
        try: # Retry once if failed in case the socket has just gone bad.
            self._connection.request("POST", self._selector_url, xml_request, headers)
            response = self._connection.getresponse()
        except (httplib.CannotSendRequest, httplib.BadStatusLine):
            Debug.warn("\nRestarting socket, resending message!")
            self._open_connection()
            self._connection.request("POST", self._selector_url, xml_request, headers)
            response = self._connection.getresponse()
        data = response.read()
        return data

    def _send_socket_request(self, xml_request):
        """ Send a request via protobuf.

            Args:
                xml_request -- A fully formed xml request string for the CPS.

            Returns:
                The raw xml response string.
        """
        def to_variant(number):
            buff = []
            while number:
                byte = number % 128
                number = number // 128
                if number > 0:
                    byte |= 0x80
                buff.append(chr(byte))
            return ''.join(buff)

        def from_variant(stream):
            used = 0
            number = 0
            q = 1
            while True:
                byte = ord(stream[used])
                used += 1
                number += q * (byte & 0x7F)
                q *= 128
                if byte&0x80==0:
                    break
            return (number, used)

        def encode_fields(fields):
            chunks = []
            for field_id, message in fields.items():
                chunks.append(to_variant((field_id << 3) | 2)) # Hardcoded WireType=2
                chunks.append(to_variant(len(message)))
                chunks.append(message)
            return ''.join(chunks)

        def decode_fields(stream):
            fields = {}
            offset = 0
            stream_lenght = len(stream)
            while offset<stream_lenght:
                field_header, used = from_variant(stream[offset:])
                offset += used
                wire_type = field_header & 0x07
                field_id = field_header >> 3
                if wire_type==2:
                    message_lenght, used = from_variant(stream[offset:])
                    offset += used
                    fields[field_id] = stream[offset:offset+message_lenght]
                    offset += message_lenght
                elif wire_type==0:
                    fields[field_id], used = from_variant(stream[offset:])
                    offset += used
                elif wire_type==1:
                    fields[field_id] = stream[offset:offset+8]
                    offset += 8
                elif wire_type==3:
                    raise ConnectionError()
                elif wire_type==4:
                    raise ConnectionError()
                elif wire_type==5:
                    fields[field_id] = stream[offse:offset+4]
                    offset += 4
                else:
                    raise ConnectionError()
            return fields


        def make_header(lenght):
            result = []
            result.append(chr((lenght & 0x000000FF)))
            result.append(chr((lenght & 0x0000FF00) >> 8))
            result.append(chr((lenght & 0x00FF0000) >> 16))
            result.append(chr((lenght & 0xFF000000) >> 24))
            return '\t\t\x00\x00' + ''.join(result)

        def parse_header(header):
            if len(header) == 8 and header[0] == '\t' and header[1] == '\t' and\
                    header[2] == '\00' and header[3] == '\00':
                return ord(header[4]) | (ord(header[5]) << 8) |\
                        (ord(header[6]) << 16) | (ord(header[7]) << 24)
            else:
                raise ConnectionError()

        def socket_send(data):
            sent_bytes = 0
            failures = 0
            total_bytes = len(data)
            while sent_bytes < total_bytes:
                sent = self._connection.send(data[sent_bytes:])
                if sent == 0:
                    failures += 1
                    if failures > 5:
                        raise ConnectionError()
                    continue
                sent_bytes += sent

        def socket_recieve(lenght):
            total_recieved = 0
            failures = 5
            recieved_chunks = []
            while total_recieved<lenght:
                chunk = self._connection.recv(lenght-total_recieved)
                if not chunk:
                    failures += 1
                    if failures > 5:
                        raise ConnectionError()
                    continue
                recieved_chunks.append(chunk)
                total_recieved += len(chunk)
            return ''.join(recieved_chunks)

        encoded_message = encode_fields({1: xml_request,
                                         2: self._storage if self._storage else "special:detect-storage"})
        header = make_header(len(encoded_message))

        try: # Retry once if failed in case the socket has just gone bad.
            socket_send(header+encoded_message)
        except (ConnectionError, socket.error):
            self._connection.close()
            self._open_connection()
            socket_send(header+encoded_message)

        # TODO: timeout
        header = socket_recieve(8)
        lenght = parse_header(header)
        encoded_response = socket_recieve(lenght)
        response = decode_fields(encoded_response)
        # TODO: Test for id=3 error message
        # TODO: check for and raise errors 
        return response[1]


# Data manipulation methods
    def insert(self, *args, **kwargs):
        """ Insert a new document in the Clusterpoint Storage.

        Args:
            documents -- If fully_formed is False (default), accepts dict where keys are document ids and values can be ether
                        xml string, etree.ElementTree or dict representation of an xml document (see dict_to_etree()).
                        If fully_formed is True, accepts list or single document where ids are integrated in document or
                        not needed and document has the right root tag.

        Keyword args:
            fully_formed  -- If documents are fully formed (contains the right root tags and id fields) set to True
                        to avoid the owerhead of documets beeing parsed at all. If set to True only list of documents or
                        a single document can be pased as 'documents', not a dict of documents. Default is False.

        Returns:
            A ModifyResponse object.
        """
        return InsertRequest(self, *args, **kwargs).send()

    def replace(self, *args, **kwargs):
        """ Replace an existing document in the Clusterpoint Storage based on the id field. Works only if id exists!

        Args:
            See insert()

        Keyword args:
            See Request.__init__().

        Returns:
            A ModifyResponse object.
        """
        return ReplaceRequest(self, *args, **kwargs).send()

    def partial_replace(self, *args, **kwargs):
        """ Update the contents of an existing document in the Clusterpoint Storage based on the id field. Works only if id exists!

        Args:
            See insert()

        Keyword args:
            See Request.__init__().

        Returns:
            A ModifyResponse object.
        """
        return PartialReplaceRequest(self, *args, **kwargs).send()

    def update(self, *args, **kwargs):
        """ Replace an existing document in the Clusterpoint Storage based on the id field and create a new one if no match.

        Args:
            See insert()

        Keyword args:
            See Request.__init__().

        Returns:
            A ModifyResponse object.
        """
        return UpdateRequest(self, *args, **kwargs).send()

    def delete(self, *args, **kwargs):
        """ Deletes a document with the specified ID from the Clusterpoint Storage.

        Args:
            doc_ids -- Single document id or a list of them to be deleted.

        Keyword args:
            See Request.__init__().

        Returns:
            A ModifyResponse object.
        """
        return DeleteRequest(self, *args, **kwargs).send()

    def search_delete(self, *args, **kwargs):
        """ Delete the documents that would be returned to the result set by a search command using the same parameters.

        Args:
            See search()

        Keyword args:
            See Request.__init__().

        Returns:
            A SearchDeleteResponse object.
        """
        return SearchDeleteRequest(self, *args, **kwargs).send()

    def reindex(self, **kwargs):
        """ Reindex all of the documents already in the Storage.

        Keyword args:
            See Request.__init__().

        Returns:
            A Response object.
        """
        return Request(self, 'reindex', **kwargs).send()

    def backup(self, *args, **kwargs):
        """ Back up the contents of a Storage to a single backup file.

        Args:
            backup_file --  String with full path of the backup archive to be created.
                        File name must end with '.tar.gz'.

        Keyword args:
            backup_type -- Backup type string, can be ether 'full' or 'incremental'.
            See Request.__init__().

        Returns:
            A Response object.
        """
        return BackupRequest(self, *args, **kwargs).send()

    def restore(self, *args, **kwargs):
        """ Restore the contents of a Storage from a single backup file, that was previously created using a backup command.

        Args:
            backup_file -- String with full path of the backup archive from whicht to restore.
                        File name must end with '.tar.gz'.

        Keyword args:
            sequence_check --  Check for valid incremental backup sequence if True.
                        Default is True.
            See Request.__init__().

        Returns:
            A Response object.
            """
        return RestoreRequest(self, *args, **kwargs).send()

    def clear(self, **kwargs):
        """ Delete the entire contents (except logs) of a Storage.

        Keyword args:
            See Request.__init__().

        Returns:
            A Response object.
        """
        return Request(self, 'clear', **kwargs).send()

# Monitoring methods
    def status(self, **kwargs):
        """ Get  status information of the Storage.

        Keyword args:
            See Request.__init__().

        Returns:
            A Response object.
        """
        return Request(self, 'status', **kwargs).send()

# Data retrieval methods
    def search(self, *args, **kwargs):
        """ Perform a full text search in the Storage.

        Args:
            query -- A query string where all <, > and & characters that aren't supposed to be XML
                    tags, should be escaped or a dict where keys are query xpaths and values ether
                    query texts or nested dicts.
                    (see term()).

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
            group_size -- Maximum number of documents returned from one group. Default id 0 )no grouping performed).
            See Request.__init__().

        Returns:
            A SearchResponse object.
        """
        return SearchRequest(self, *args, **kwargs).send()

    def retrieve(self, *args, **kwargs):
        """ Return a document with the specified ID from the Storage.
            Error if a document with this ID does not exist in the Storage.

        Args:
            doc_ids -- Single document id or a list of them.

        Keyword args:
            See Request.__init__()

        Returns:
            A ListResponse object.
            See insert()

        """
        return RetrieveRequest(self, *args, **kwargs).send()

    def similar(self, *args, **kwargs):
        """ Search for documents that are similar to directly supplied text or to the textual content of an existing document.

        Args:
            docid -- ID of the source document - the one that You want to search similar documents to.
            len -- Number of keywords to extract from the source.
            quota -- Minimum number of keywords matching in the destination.

        Keyword args:
            offset -- Number of results to skip before returning the following ones.
            docs -- Number of documents to retrieve. Default is 10.
            query -- An optional query that all found documents have to match against. See Search().
            See Request.__init__()

        Returns:
            A ListResponse object.
        """
        return SimilarRequest(self, *args, **kwargs).send()

    def similar_text(self, *args, **kwargs):
        """ Search for documents that are similar to directly supplied text or to the textual content of an existing document.

        Args:
            text -- Text to found something similar to.
            len -- Number of keywords to extract from the source.
            quota -- Minimum number of keywords matching in the destination.

        Keyword args:
            offset -- Number of results to skip before returning the following ones.
            docs -- Number of documents to retrieve. Default is 10.
            query -- An optional query that all found documents have to match against. See Search().
            See Request.__init__()

        Returns:
            A ListResponse object.
        """
        return SimilarRequest(self, *args, mode='text', **kwargs).send()

    def lookup(self, *args, **kwargs):
        """  Search for a document in the Storage by it's id.

        Args:
            doc_ids -- Single document id or a list of them.

        Keyword args:
            list -- Defines which tags of the search results should be listed in the response.
                    A dict with tag xpaths as keys and listing option strings ('yes', 'no', 'snippet', 'highlight') as values.
            See Request.__init__()

        Returns:
            A LookupResponse object.
        """
        return LookupRequest(self, *args, **kwargs).send()

    def alternatives(self, *args, **kwargs):
        """ Return a set of words from the Storage vocabulary that are alternatives to the given word.

        Args:
            query -- A query string where all <, > and & characters that aren't supposed to be XML
                    tags, should be escaped or a dict where keys are query xpaths and values ether
                    query texts or nested dicts.
                    (see term()).

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

        Returns:
            A AlternativesResponse object.
        """
        return AlternativesRequest(self, *args, **kwargs).send()

    def list_words(self, *args, **kwargs):
        """  Return the list of words corresponding to a search query, i.e. all the words that could possibly match a search query.

        Args:
            query -- A query string where all <, > and & characters that aren't supposed to be XML
                    tags, should be escaped or a dict where keys are query xpaths and values ether
                    query texts or nested dicts.
                    (see term()).

        Keyword args:
            See Request.__init__()

        Returns:
            A WordsResponse object.
        """
        return ListWordsRequest(self, *args, **kwargs).send()

    def list_first(self, *args, **kwargs):
        """ Search for documents in the Storage that have been inserted, updated, or replaced the longest time ago.
        Keyword args:
                list -- Defines which tags of the search results should be listed in the response.
                        A dict with tag xpaths as keys and listing option strings ('yes', 'no', 'snippet',
                        'highlight') as values.
                docs -- Number of documents to be returned. Default is 10.
                offset -- Offset from the beginning of the result set. Default is 0.
                See Request.__init__()

        Returns:
            A ListResponse object.
        """
        return ListFirstRequest(self, *args, **kwargs).send()

    def list_last(self, *args, **kwargs):
        """ Search for documents in the Storage that have been inserted, updated, or replaced most recently.

        Keyword args:
                list -- Defines which tags of the search results should be listed in the response.
                        A dict with tag xpaths as keys and listing option strings ('yes', 'no', 'snippet',
                        'highlight') as values.
                docs -- Number of documents to be returned. Default is 10.
                offset -- Offset from the beginning of the result set. Default is 0.
                See Request.__init__()

        Returns:
            A ListResponse object.
        """
        return ListLastRequest(self, *args, **kwargs).send()

    def retrieve_last(self, *args, **kwargs):
        """ Return compleate documents that are most recently added to the Storage.

        Keyword args:
                docs -- Number of documents to be returned. Default is 10.
                offset -- Offset from the beginning of the result set. Default is 0.
                See Request.__init__()

        Returns:
            A ListResponse object.
        """
        return RetrieveLastRequest(self, *args, **kwargs).send()

    def retrieve_first(self, *args, **kwargs):
        """ Return compleate documents that are added to the Storeage the longest time ago.

        Keyword args:
                docs -- Number of documents to be returned. Default is 10.
                offset -- Offset from the beginning of the result set. Default is 0.
                See Request.__init__()

        Returns:
            A ListResponse object.
        """
        return RetrieveFirstRequest(self, *args, **kwargs).send()


    def list_paths(self, **kwargs):
        """ Get a list of all xpaths that are available to the storage.

        Keyword args:
            See Request.__init__().

        Returns:
            A ListPathsResponse object.
        """
        return Request(self, 'list-paths', **kwargs).send()


    def list_facets(self, *args, **kwargs):
        """ Get a list of all terms that the storage has seen for a particular facet (or multiple facets).

            Args:
                paths -- A single facet path string or a list of them.

        Keyword args:
            See Request.__init__().

        Returns:
            A ListFacetsResponse object.
        """
        return ListFacetsRequest(self, *args, **kwargs).send()
