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

from __future__ import print_function


class Debug(object):
    """Class for printing colored development debuging indormation."""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    BOLD = '\033[1m'
    ENDC = '\033[0m'

    _DEBUG = False

    @classmethod
    def dump(cls, name, text):
        if cls._DEBUG:
            print(cls.HEADER + str(name) + cls.ENDC + str(text) + '\n')

    @classmethod
    def warn(cls, text):
        if cls._DEBUG:
            print(cls.WARNING + text + cls.ENDC + '\n')

    @classmethod
    def ok(cls, text):
        if cls._DEBUG:
            print(cls.OKGREEN + text + cls.ENDC + '\n')

    @classmethod
    def fail(cls, text):
        if cls._DEBUG:
            print(cls.FAIL + text + cls.ENDC + '\n')


#TODO: Do we need it?
class RawXML(object):
    """Wrap strings to indicate that they are already formed XML and not to be escaped."""

    def __init__(self, raw_string):
        """Create wraper object and store the raw string in it.

        Args:
            raw_string -- An already formated XML string to be nested in a document.

        Attrbutes:
            raw_string -- The wraped string.
    """
        self.raw_string = raw_string
