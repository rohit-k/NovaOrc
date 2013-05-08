# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2013 NTT Data.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""
Code for zookeeper message dispatching.
"""

from nova.orc.zk import common as zk_common


class ZkDispatcher(object):
    """Dispatch zk messages according to the requested API version.

    This class can be used as the top level 'manager' for a service.  It
    contains a list of underlying managers that have an API_VERSION attribute.
    """

    def __init__(self, callbacks):
        """Initialize the zk dispatcher.

        :param callbacks: List of proxy objects that are an instance
                          of a class with zk methods exposed.  Each proxy
                          object should have an ZK_API_VERSION attribute.
        """
        self.callbacks = callbacks
        super(ZkDispatcher, self).__init__()

    def dispatch(self, ctxt, version, method, **kwargs):
        """Dispatch a message based on a requested version.

        :param ctxt: The request context
        :param version: The requested API version from the incoming message
        :param method: The method requested to be called by the incoming
                       message.
        :param kwargs: A dict of keyword arguments to be passed to the method.

        :returns: Whatever is returned by the underlying method that gets
                  called.
        """
        if not version:
            version = '1.0'

        had_compatible = False
        for proxyobj in self.callbacks:
            if hasattr(proxyobj, 'ZK_API_VERSION'):
                zk_api_version = proxyobj.ZK_API_VERSION
            else:
                zk_api_version = '1.0'
            is_compatible = zk_common.version_is_compatible(zk_api_version,
                                                             version)
            had_compatible = had_compatible or is_compatible
            if not hasattr(proxyobj, method):
                continue
            if is_compatible:
                return getattr(proxyobj, method)(ctxt, **kwargs)

        if had_compatible:
            raise AttributeError("No such zk function '%s'" % method)
        else:
            raise zk_common.UnsupportedZkVersion(version=version)
