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
# along with XRootD.  If not, see <http:#www.gnu.org/licenses/>.
#-------------------------------------------------------------------------------

import os
import fnmatch
import glob as gl
from threading import Lock
from urllib.parse import urlparse
from XRootD.client.responses import XRootDStatus, HostList
from XRootD.client.filesystem import FileSystem

class CallbackWrapper(object):
  def __init__(self, callback, responsetype):
    if not hasattr(callback, '__call__'):
      raise TypeError('callback must be callable function, class or lambda')
    self.callback = callback
    self.responsetype = responsetype

  def __call__(self, status, response, *argv):
    self.status = XRootDStatus(status)
    self.response = response
    if self.responsetype:
      self.response = self.responsetype(response)
    if argv:
      self.hostlist = HostList(argv[0])
    else:
      self.hostlist = HostList([])
    self.callback(self.status, self.response, self.hostlist)

class AsyncResponseHandler(object):
  """Utility class to handle asynchronous method calls."""
  def __init__(self):
    self.mutex = Lock()
    self.mutex.acquire()

  def __call__(self, status, response, hostlist):
    self.status = status
    self.response = response
    self.hostlist = hostlist
    self.mutex.release()

  def wait(self):
    """Block and wait for the async response"""
    self.mutex.acquire()
    self.mutex.release()
    return self.status, self.response, self.hostlist

class CopyProgressHandler(object):
  """Utility class to handle progress updates from copy jobs

  .. note:: This class does nothing by itself. You have to subclass it and do
            something useful with the progress updates yourself.
  """

  def begin(self, jobId, total, source, target):
    """Notify when a new job is about to start

    :param  jobId: the job number of the copy job concerned
    :type   jobId: integer
    :param  total: total number of jobs being processed
    :type   total: integer
    :param source: the source url of the current job
    :type  source: :mod:`XRootD.client.URL` object
    :param target: the destination url of the current job
    :type  target: :mod:`XRootD.client.URL` object
    """
    pass

  def end(self, jobId, results):
    """Notify when the previous job has finished

    :param  jobId: the job number of the copy job concerned
    :type   jobId: integer
    :param status: status of the job
    :type  status: :mod:`XRootD.client.responses.XRootDStatus` object
    """
    pass

  def update(self, jobId, processed, total):
    """Notify about the progress of the current job

    :param     jobId: the job number of the copy job concerned
    :type      jobId: integer
    :param processed: bytes processed by the current job
    :type  processed: integer
    :param     total: total number of bytes to be processed by the current job
    :type      total: integer
    """
    pass


  def should_cancel( self, jobId ):
    """Check whether the current job should be canceled.

    :param  jobId: the job number of the copy job concerned
    :type   jobId: integer
    """
    return False


def split_url(url):
  """Split a URL into domain and path components.

  :param url: The URL to split
  :type  url: string
  :returns:   tuple of (domain, path)
  """
  parsed_uri = urlparse(url)
  domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
  path = parsed_uri.path
  return domain, path


def extract_url_params(pathname):
  """Extract URL parameters from a pathname, distinguishing them from glob patterns.

  URL parameters are identified as the rightmost '?' followed by key=value pairs.
  Returns (pathname_without_params, url_params_string)

  :param pathname: The pathname potentially containing URL parameters
  :type  pathname: string
  :returns:        tuple of (pathname_without_params, url_params_string)
  """
  # Look for '?' followed by something that looks like URL parameters (contains '=')
  # We search from the right to avoid catching glob '?' wildcards
  idx = pathname.rfind('?')
  if idx == -1:
    return pathname, ''

  # Check if what follows looks like URL parameters (contains '=')
  potential_params = pathname[idx+1:]
  if '=' in potential_params:
    # This looks like URL parameters, not a glob pattern
    return pathname[:idx], pathname[idx:]

  # It's just a glob wildcard, not URL parameters
  return pathname, ''


def iglob(pathname, raise_error=False):
  """Generates paths based on a wild-carded path, potentially via xrootd.

  Multiple wild-cards can be present in the path.

  :param  pathname: The wild-carded path to be expanded.
  :type   pathname: string
  :param raise_error: Whether or not to let xrootd raise an error if
                      there's a problem. If False (default), and there's a problem for a
                      particular directory or file, then that will simply be skipped,
                      likely resulting in an empty list.
  :type  raise_error: boolean
  :yields: A single path that matches the wild-carded string
  """
  # Extract URL parameters before processing
  pathname_clean, url_params = extract_url_params(pathname)

  # Let normal python glob try first
  generator = gl.iglob(pathname_clean)
  path = next(generator, None)
  if path is not None:
    yield path + url_params
    for path in generator:
      yield path + url_params
    return

  # Else try xrootd instead
  for path in xrootd_iglob(pathname_clean, url_params, raise_error=raise_error):
    yield path


def xrootd_iglob(pathname, url_params, raise_error):
  """Handles the actual interaction with xrootd

  Provides a python generator over files that match the wild-card expression.

  :param  pathname: The wild-carded path to be expanded (without URL parameters)
  :type   pathname: string
  :param url_params: The URL parameters string (including leading '?')
  :type  url_params: string
  :param raise_error: Whether or not to let xrootd raise an error if there's a problem
  :type  raise_error: boolean
  :yields: A single path that matches the wild-carded string
  """
  # Split the pathname into a directory and basename
  dirs, basename = os.path.split(pathname)

  if gl.has_magic(dirs):
    dirs = list(xrootd_iglob(dirs, url_params, raise_error))
  else:
    dirs = [dirs]

  for dirname in dirs:
    host, path = split_url(dirname)
    query = FileSystem(host)

    if not query:
      raise RuntimeError("Cannot prepare xrootd query")

    # Include URL parameters in the dirlist call for authentication
    # Note: The "/" is important for proper path construction
    status, dirlist = query.dirlist(path + "/" + url_params)
    if status.error:
      if not raise_error:
        continue
      raise RuntimeError("'{!s}' for path '{}'".format(status, dirname))

    for entry in dirlist.dirlist:
      filename = entry.name
      if filename in [".", ".."]:
        continue
      if not fnmatch.fnmatchcase(filename, basename):
        continue
      # Append URL parameters to the final path
      yield os.path.join(dirname, filename) + url_params


def glob(pathname, raise_error=False):
  """Creates a list of paths that match pathname.

  Multiple wild-cards can be present in the path.

  :param  pathname: The wild-carded path to be expanded.
  :type   pathname: string
  :param raise_error: Whether or not to let xrootd raise an error if
                      there's a problem. If False (default), and there's a problem for a
                      particular directory or file, then that will simply be skipped,
                      likely resulting in an empty list.
  :type  raise_error: boolean
  :returns: A list of paths that match the wild-carded string
  """
  return list(iglob(pathname, raise_error=raise_error))
