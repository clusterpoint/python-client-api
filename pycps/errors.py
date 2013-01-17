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


class CPSWarning(Warning):
    """ Base class for warnings in this module. """
    pass


class CPSError(Exception):
    """ Base class for exceptions in this module. """
    pass


class ConnectionError(CPSError):
    """ Exception raised for connection problems with the Clusterpoint Storage.

    Attributes:
        message -- An optional error message.
    """
    def __init__(self, message=None):
        self.message = message

    def __str__(self):
        return "Unable to connect to the Clusterpoint server! " + self.message


class XMLError(CPSError):
    """Exception raised for bad XML document.

    Attributes:
        dump -- An optional xml dump.
    """
    def __init__(self, dump=None):
        self.dump = dump

    def __str__(self):
        if self.dump:
            return "Bad xml document:\n" + self.dump
        else:
            return "Bad xml document, unable to parse!"


class ParameterError(CPSError):
    """Exception raised for bad parameter values.

    Attributes:
        dump -- An optional parameter dump.
    """
    def __init__(self, dump=None):
        self.dump = dump

    def __str__(self):
        if self.dump:
            return "Bad parameter:\n" + self.dump
        else:
            return "Bad parameter!"


class APIError(CPSError):
    """Exception raised for CPS API errors possibly indicating data loss.

    Attributes:
        code -- Error code.
        text -- Error textual message.
        level -- Error severity.
        source -- Subsystem in which the error occurred.
        message -- Longer error message.
        document_id -- List of ocument_ids that the error refers to.
                    Present only on some errors.
    """
    def __init__(self, xml_error):
        self.code = int(xml_error.find('code').text)
        self.text = xml_error.find('text').text
        self.level = xml_error.find('level').text
        self.source = xml_error.find('source').text
        self.message = xml_error.find('message').text
        self.document_id = [document_id.text for document_id in xml_error.findall('document_id')]

    def __str__(self):
        return self.text


class APIWarning(CPSWarning):
    """Warning raised for CPS API errors that shouldn't have resulted in data loss.

    Attributes:
        code -- Error code.
        text -- Error textual message.
        level -- Error severity.
        source -- Subsystem in which the error occurred.
        message -- Longer error message.
        document_id -- List of ocument_ids that the error refers to.
                    Present only on some errors.
    """
    def __init__(self, xml_error):
        self.code = xml_error.find('code').text
        self.text = xml_error.find('text').text
        self.level = xml_error.find('level').text
        self.source = xml_error.find('source').text
        self.message = xml_error.find('message').text
        self.document_id = [document_id.text for document_id in xml_error.findall('document_id')]

    def __str__(self):
        return self.text


class ResponseError(CPSError):
    """Exception raised for invalid Clusterpoint response strings.

    Attributes:
        response -- The xml response string.
    """
    def __init__(self, response):
        self.response = response

    def __str__(self):
        return "Invalid XML response recieved: " + self.response
