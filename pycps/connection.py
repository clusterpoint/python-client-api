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

from request import *


class Connection(object):
    """ Connection object class to hold connections with CPS and use them.

    .. note:: For now only http protocol suported!

    """

    def __init__(self, host, port, storage, user, password,
                 document_root_xpath='document', document_id_xpath='./id',
                 selector_url='/cgi-bin/cps2-cgi', application='PYCPS',  reply_charset=None):
        """
            Args:
                host -- A host address string.
                port -- A host port number.
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

        self._host = host
        self._port = port   # XXX: should be just included in a url string with host and type.
        self._storage = storage
        self._user = user
        self._password = password
        self._selector_url = selector_url

        self.document_root_xpath = document_root_xpath
        self.document_id_xpath = document_id_xpath
        self.application = application
        self.reply_charset = reply_charset

        self._open_connection()

    def _open_connection(self):
        """ Select and use the right connection method to connect to the CPS."""
        #TODO: add other protocols
        try:
            self._connection = self._open_http_connection()
        except:
            raise ConnectionError()

    def _open_http_connection(self):
        """ Open a new connection socket to the CPS using the HTTP protocol."""
        return httplib.HTTPConnection(self._host, self._port, strict=False)

    def _send_request(self, xml_request):
        """ Send the prepared XML request block to the CPS using the corect protocol.

            Args:
                xml_request -- A fully formed xml request string for the CPS.

            Returns:
                The raw xml response string.

            Raises:
                ConnectionError -- Can't establish a connection with the server.
        """
        try:
            return self._send_http_request(xml_request)
        except:
            raise ConnectionError()

    def _send_http_request(self, xml_request):
        """ Send a request via HTTP protocol.

            Args:
                xml_request -- A fully formed xml request string for the CPS.

            Returns:
                The raw xml response string.
        """
        headers = {"Host": self._host, "Content-Type": "text/xml", "Recipient": self._storage}
        try:
            self._connection.request("POST", self._selector_url, xml_request, headers)
            response = self._connection.getresponse()
        except (httplib.CannotSendRequest, httplib.BadStatusLine):
            Debug.warn("\nRestarting socket, resending message!")
            self._open_connection()
            self._connection.request("POST", self._selector_url, xml_request, headers)
            response = self._connection.getresponse()
        data = response.read()
        return data

# Data manipulation methods
    def insert(self, *args, **kwargs):
        """ Insert a new document in the Clusterpoint Storage.

        Args:
                documents -- A dict where keys are document ids and values can be ether xml string, etree.ElementTree or dict
                            representation of an xml document (see dict_to_etree()). If Ids are integrated in document or not needed,
                            use add_ids=False and pass list of documents or single document instead of the dict.

            Keyword args:
                add_ids -- If True argument must be dict with document ids as keys, that will be inserted in documents.
                            Default is True.
                See :class:`Request`

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
