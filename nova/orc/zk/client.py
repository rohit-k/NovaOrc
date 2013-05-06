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

import sys
import uuid

from eventlet import greenpool
from eventlet import pools
from eventlet import semaphore


from nova.openstack.common.gettextutils import _
from nova.openstack.common import local
from nova.openstack.common import log as logging
from nova.orc.zk import common as zk_common

LOG = logging.getLogger(__name__)


class Pool(pools.Pool):
    """Class that implements a Pool of Connections."""
    def __init__(self, conf, connection_cls, *args, **kwargs):
        self.connection_cls = connection_cls
        self.conf = conf
        kwargs.setdefault("max_size", 1000)
        kwargs.setdefault("order_as_stack", True)
        super(Pool, self).__init__(*args, **kwargs)
        self.reply_proxy = None

    # TODO(comstud): Timeout connections not used in a while
    def create(self):
        LOG.debug(_('Pool creating new connection'))
        return self.connection_cls(self.conf)

    def empty(self):
        while self.free_items:
            self.get().close()
        # Force a new connection pool to be created.
        # Note that this was added due to failing unit test cases. The issue
        # is the above "while loop" gets all the cached connections from the
        # pool and closes them, but never returns them to the pool, a pool
        # leak. The unit tests hang waiting for an item to be returned to the
        # pool. The unit tests get here via the teatDown() method. In the run
        # time code, it gets here via cleanup() and only appears in service.py
        # just before doing a sys.exit(), so cleanup() only happens once and
        # the leakage is not a problem.
        self.connection_cls.pool = None


class ZkContext(zk_common.CommonZkContext):
    """Context that supports replying to a rpc.call"""
    def __init__(self, **kwargs):
        self.msg_id = kwargs.pop('msg_id', None)
        self.reply_q = kwargs.pop('reply_q', None)
        self.conf = kwargs.pop('conf')
        super(ZkContext, self).__init__(**kwargs)

    def deepcopy(self):
        values = self.to_dict()
        values['conf'] = self.conf
        values['msg_id'] = self.msg_id
        values['reply_q'] = self.reply_q
        return self.__class__(**values)


def unpack_context(conf, msg):
    """Unpack context from msg."""
    context_dict = {}
    for key in list(msg.keys()):
        # NOTE(vish): Some versions of python don't like unicode keys
        #             in kwargs.
        key = str(key)
        if key.startswith('_context_'):
            value = msg.pop(key)
            context_dict[key[9:]] = value
    context_dict['msg_id'] = msg.pop('_msg_id', None)
    context_dict['reply_q'] = msg.pop('_reply_q', None)
    context_dict['conf'] = conf
    ctx = ZkContext.from_dict(context_dict)
    zk_common._safe_log(LOG.debug, _('unpacked context: %s'), ctx.to_dict())
    return ctx


def pack_context(msg, context):
    """Pack context into msg.

    Values for message keys need to be less than 255 chars, so we pull
    context out into a bunch of separate keys. If we want to support
    more arguments in zookeeper messages, we may want to do the same
    for args at some point.

    """
    context_d = dict([('_context_%s' % key, value)
                      for (key, value) in context.to_dict().iteritems()])
    msg.update(context_d)


_pool_create_sem = semaphore.Semaphore()


def get_connection_pool(conf, connection_cls):
    with _pool_create_sem:
        # Make sure only one thread tries to create the connection pool.
        if not connection_cls.pool:
            connection_cls.pool = Pool(conf, connection_cls)
    return connection_cls.pool


class ConnectionContext(zk_common.Connection):
    """The class that is actually returned to the caller of
    create_connection().  This is essentially a wrapper around
    Connection that supports 'with'.  It can also return a new
    Connection, or one from a pool.  The function will also catch
    when an instance of this class is to be deleted.  With that
    we can return Connections to the pool on exceptions and so
    forth without making the caller be responsible for catching
    them.  If possible the function makes sure to return a
    connection to the pool.
    """

    def __init__(self, conf, connection_pool, pooled=True, server_params=None):
        """Create a new connection, or get one from the pool"""
        self.connection = None
        self.conf = conf
        self.connection_pool = connection_pool
        if pooled:
            self.connection = connection_pool.get()
        else:
            self.connection = connection_pool.connection_cls(conf,
                                                  server_params=server_params)
        self.pooled = pooled

    def __enter__(self):
        """When with ConnectionContext() is used, return self"""
        return self

    def _done(self):
        """If the connection came from a pool, clean it up and put it back.
        If it did not come from a pool, close it.
        """
        if self.connection:
            if self.pooled:
                # Reset the connection so it's ready for the next caller
                # to grab from the pool
                self.connection.reset()
                self.connection_pool.put(self.connection)
            else:
                try:
                    self.connection.close()
                except Exception:
                    pass
            self.connection = None

    def __exit__(self, exc_type, exc_value, tb):
        """End of 'with' statement.  We're done here."""
        self._done()

    def __del__(self):
        """Caller is done with this connection.  Make sure we cleaned up."""
        self._done()

    def close(self):
        """Caller is done with this connection."""
        self._done()

    def create_consumer(self, queue, proxy):
        self.connection.create_consumer(queue, proxy)

    def consume_in_thread(self):
        self.connection.consume_in_thread()

    def __getattr__(self, key):
        """Proxy all other calls to the Connection instance"""
        if self.connection:
            return getattr(self.connection, key)
        else:
            raise zk_common.InvalidZKConnectionReuse()


class _ThreadPoolWithWait(object):
    """Base class for a delayed invocation manager used by
    the Connection class to start up green threads
    to handle incoming messages.
    """

    def __init__(self, conf, connection_pool):
        self.pool = greenpool.GreenPool(conf.rpc_thread_pool_size)
        self.connection_pool = connection_pool
        self.conf = conf

    def wait(self):
        """Wait for all callback threads to exit."""
        self.pool.waitall()


class ProxyCallback(_ThreadPoolWithWait):
    """Calls methods on a proxy object based on method and args."""

    def __init__(self, conf, proxy, connection_pool):
        super(ProxyCallback, self).__init__(
            conf=conf,
            connection_pool=connection_pool,
        )
        self.proxy = proxy

    def __call__(self, message_data):
        """Consumer callback to call a method on a proxy object.

        Parses the message for validity and fires off a thread to call the
        proxy object method.

        Message data should be a dictionary with two keys:
            method: string representing the method to call
            args: dictionary of arg: value

        Example: {'method': 'echo', 'args': {'value': 42}}

        """
        # It is important to clear the context here, because at this point
        # the previous context is stored in local.store.context
        if hasattr(local.store, 'context'):
            del local.store.context
        zk_common._safe_log(LOG.debug, _('received %s'), message_data)
        ctxt = unpack_context(self.conf, message_data)
        method = message_data.get('method')
        args = message_data.get('args', {})
        version = message_data.get('version', None)
        if not method:
            LOG.warn(_('no method for message: %s') % message_data)
            return
        self.pool.spawn_n(self._process_data, ctxt, version, method, args)

    def _process_data(self, ctxt, version, method, args):
        """Process a message in a new thread.

        If the proxy object we have has a dispatch method
        (see rpc.dispatcher.RpcDispatcher), pass it the version,
        method, and args and let it dispatch as appropriate.  If not, use
        the old behavior of magically calling the specified method on the
        proxy we have here.
        """
        ctxt.update_store()
        try:
            self.proxy.dispatch(ctxt, version, method, **args)
        except zk_common.ClientException as e:
            LOG.debug(_('Expected exception during message handling (%s)') %
                      e._exc_info[1])
        except Exception:
            # sys.exc_info() is deleted by LOG.exception().
            exc_info = sys.exc_info()
            LOG.error(_('Exception during message handling'),
                      exc_info=exc_info)


def _add_unique_id(msg):
    """Add unique_id for checking duplicate messages."""
    unique_id = uuid.uuid4().hex
    msg.update({'UNIQUE_ID': unique_id})
    LOG.debug(_('UNIQUE_ID is %s.') % (unique_id))


def put_message_in_queue(conf, context, queue, msg, connection_pool):
    """Sends a message on a queue without waiting for a response."""
    LOG.debug(_('Making asynchronous put on %s...'), queue)
    pack_context(msg, context)
    with ConnectionContext(conf, connection_pool) as conn:
        conn.put_message_in_queue(context, queue, zk_common.serialize_msg(msg))


def create_node(conf, context, path, data, connection_pool):
    """create a node with the given value as its data."""
    pack_context(data, context)
    with ConnectionContext(conf, connection_pool) as conn:
        conn.create_node(path, zk_common.serialize_msg(data))


def get_children(conf, context, path, connection_pool):
    """Returns the children of the given node"""
    #LOG.info(_('Getting the children of the node'), path)
    with ConnectionContext(conf, connection_pool) as conn:
        return conn.get_children(path)


def get_data(conf, context, path, connection_pool):
    """Gets the data from the given path/node"""
    #LOG.info(_('Getting the data present at the node'), path)
    with ConnectionContext(conf, connection_pool) as conn:
        return conn.get_data(path)


def set_data(conf, context, path, data, connection_pool):
    """set the data of a node"""
    with ConnectionContext(conf, connection_pool) as conn:
        return conn.set_data(path, data)


def create_connection(conf, new, connection_pool):
    """Create a connection"""
    return ConnectionContext(conf, connection_pool, pooled=not new)
