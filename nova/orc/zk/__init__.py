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
A zookeeper call (zk) abstraction.

For some wrappers that add message versioning,
    zk.dispatcher
    zk.proxy
"""
from oslo.config import cfg

from nova.openstack.common import importutils

zk_opts = [
    cfg.StrOpt('zk_backend',
               default='%s.impl_kazoo' % __package__,
               help="The zookeeper backend to use, defaults to kazoo."),
]

CONF = cfg.CONF
CONF.register_opts(zk_opts)


def put_message_in_queue(context, queue, msg):
    """Invoke a remote method that does not return anything.

    :param context: Information that identifies the user that has made this
                    request.
    :param queue: The queue to send the message to.
    :param msg: This is a dict in the form { "method" : "method_to_invoke",
                                             "args" : dict_of_kwargs }

    :returns: None
    """
    return _get_impl().put_message_in_queue(CONF, context, queue, msg)


def create_node(context, path, data):
    return _get_impl().create_node(CONF, context, path, data)


def get_children(context, path):
    return _get_impl().get_children(CONF, context, path)


def get_data(context, path):
    return _get_impl().get_data(CONF, context, path)


def set_data(context, path, data):
    return _get_impl().set_data(CONF, context, path, data)


def create_connection(new=True):
    """Create a connection to the message bus used for rpc.

    For some example usage of creating a connection and some consumers on that
    connection, see nova.service.

    :param new: Whether or not to create a new connection.  A new connection
                will be created by default.  If new is False, the
                implementation is free to return an existing connection from a
                pool.

    :returns: An instance of openstack.common.rpc.common.Connection
    """
    return _get_impl().create_connection(CONF, new=new)


_ZKIMPL = None


def _get_impl():
    """Delay import of zk_backend until configuration is loaded."""
    global _ZKIMPL
    if _ZKIMPL is None:
        _ZKIMPL = importutils.import_module(CONF.zk_backend)
    return _ZKIMPL
