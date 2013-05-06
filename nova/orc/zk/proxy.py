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
A helper class for proxy objects to remote APIs.

For more information about zk API version numbers, see:
    zk/dispatcher.py
"""

from nova.orc import zk


class ZkProxy(object):
    """A helper class for zk clients.

    This class is a wrapper around the ZK client API.  It allows you to
    specify the topic and API version in a single place.  This is intended to
    be used as a base class for a class that implements the client side of an
    zk API.
    """

    def __init__(self, queue, default_version):
        """Initialize an ZkProxy.

        :param topic: The topic to use for all messages.
        :param default_version: The default API version to request in all
               outgoing messages.  This can be overridden on a per-message
               basis.
        """
        self.queue = queue
        self.default_version = default_version
        super(ZkProxy, self).__init__()

    def _set_version(self, msg, vers):
        """Helper method to set the version in a message.

        :param msg: The message having a version added to it.
        :param vers: The version number to add to the message.
        """
        msg['version'] = vers if vers else self.default_version

    def _get_queue(self, queue):
        """Return the queue to use for a message."""
        return queue if queue else self.queue

    @staticmethod
    def make_msg(method, **kwargs):
        return {'method': method, 'args': kwargs}

    def put_message_in_queue(self, context, msg, queue=None, version=None):
        """zk.put() a remote method.

        :param context: The request context
        :param msg: The message to send, including the method and args.
        :param topic: Override the topic for this message.
        :param version: (Optional) Override the requested API version in this
               message.

        :returns: None.  rpc.put() does not wait on any return value from the
                  remote method.
        """
        self._set_version(msg, version)
        zk.put_message_in_queue(context, self._get_queue(queue), msg)

    def create_node(self, context, path, data, version=None):
        self._set_version(data, version)
        zk.create_node(context, path, data)

    def get_children(self, context, path):
        return zk.get_children(context, path)

    def get_data(self, context, path):
        return zk.get_data(context, path)

    def set_data(self, context, path, data):
        zk.set_data(context, path, data)
