#-------------------------------------------------------------------------------
# Copyright (c) 2012-2013 by European Organization for Nuclear Research (CERN)
# Author: Justin Salmon <jsalmon@cern.ch>
#-------------------------------------------------------------------------------
# XRootD is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# XRootD is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
#-------------------------------------------------------------------------------

import unittest
from XRootD.client.glob import extract_url_params, split_url


class TestGlobFunctions(unittest.TestCase):

    def test_extract_url_params_with_params(self):
        """Test extracting URL parameters from a pathname"""
        pathname = "root://server.com//path/to/file?xrd.wantprot=unix&authz=TOKEN"
        expected_path = "root://server.com//path/to/file"
        expected_params = "?xrd.wantprot=unix&authz=TOKEN"

        path, params = extract_url_params(pathname)
        self.assertEqual(path, expected_path)
        self.assertEqual(params, expected_params)

    def test_extract_url_params_without_params(self):
        """Test pathname without URL parameters"""
        pathname = "root://server.com//path/to/file"
        expected_path = "root://server.com//path/to/file"
        expected_params = ""

        path, params = extract_url_params(pathname)
        self.assertEqual(path, expected_path)
        self.assertEqual(params, expected_params)

    def test_extract_url_params_glob_wildcard(self):
        """Test that glob wildcards (?) are not confused with URL parameters"""
        pathname = "/path/to/file?.txt"
        expected_path = "/path/to/file?.txt"
        expected_params = ""

        path, params = extract_url_params(pathname)
        self.assertEqual(path, expected_path)
        self.assertEqual(params, expected_params)

    def test_extract_url_params_multiple_question_marks(self):
        """Test pathname with both glob wildcards and URL parameters"""
        pathname = "/path/to/file?.txt?param=value"
        expected_path = "/path/to/file?.txt"
        expected_params = "?param=value"

        path, params = extract_url_params(pathname)
        self.assertEqual(path, expected_path)
        self.assertEqual(params, expected_params)

    def test_split_url(self):
        """Test splitting URL into domain and path"""
        url = "root://server.com:1094//path/to/file"
        expected_domain = "root://server.com:1094/"
        expected_path = "/path/to/file"

        domain, path = split_url(url)
        self.assertEqual(domain, expected_domain)
        self.assertEqual(path, expected_path)


if __name__ == '__main__':
    unittest.main()
