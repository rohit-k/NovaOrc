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

import functools
import itertools
import json
import socket
import sys
import time

import eventlet
import greenlet

import kazoo.client
from kazoo import exceptions
from kazoo.handlers import threading
from oslo.config import cfg

from nova.openstack.common import network_utils
from nova.orc.zk import client as zk_client
from nova.orc.zk import common as zk_common


kazoo_opts = [
    cfg.ListOpt('zk_hosts',
                default='127.0.0.1:2181',
                help='comma separated host:port pairs, each corresponding to '
                'a zk server. e.g. "127.0.0.1:2181,127.0.0.1:2182"'),
    cfg.IntOpt('zk_port',
               default=2181,
               help='The port to listen for client connections'),
    cfg.IntOpt('kazoo_retry_interval',
               default=1,
               help='how frequently to retry connecting with Zookeeper'),
    cfg.IntOpt('kazoo_retry_backoff',
               default=2,
               help='how long to backoff for between retries when connecting '
                    'to Zookeeper'),
    cfg.IntOpt('kazoo_max_retries',
               default=3,
               help='maximum retries with trying to connect to Zookeeper '
                    '(the default of 0 implies an infinite retry count)'),
]

CONF = cfg.CONF
cfg.CONF.register_opts(kazoo_opts, group="zookeeper")

LOG = zk_common.LOG


class Publisher(object):
    """Base Publisher class."""

    def __init__(self, locking_queue):
        """Init the Publisher class."""
        self.locking_queue = locking_queue

    def send(self, msg, priority=100):
        """Send a message."""
        self.locking_queue.put(msg, priority)


class QueuePublisher(Publisher):
    """Publisher class for 'topic'."""
    def __init__(self, conf, locking_queue):
        """init a 'queue' publisher.

        Kombu options may be passed as keyword args to override defaults
        """
        super(QueuePublisher, self).__init__(locking_queue)


class ConsumerBase(object):
    """Consumer base class."""

    def __init__(self, locking_queue, callback, tag, **kwargs):
        """Declare a zookeeper distributed queue

        'locking_queue' is the zookeeper distributed queue to use
        'callback' is the callback to call when messages are received
        'tag' is a unique ID for the consumer

        """
        self.callback = callback
        self.locking_queue = locking_queue
        self.tag = tag

    def consume(self, *args, **kwargs):
        """Actually declare the consumer.  This will
        start the flow of messages from the queue.  Using the
        Connection.iterconsume() iterator will process the messages,
        calling the appropriate callback.

        If a callback is specified in kwargs, use that.  Otherwise,
        use the callback passed during __init__()

        If kwargs['nowait'] is True, then this call will block until
        a message is read.

        Messages will automatically be acked if the callback doesn't
        raise an exception
        """
        options = {'consumer_tag': self.tag}
        options['nowait'] = kwargs.get('nowait', False)
        callback = kwargs.get('callback', self.callback)
        if not callback:
            raise ValueError("No callback defined")

        def _callback(raw_message):
            try:
                raw_message = json.loads(raw_message)
                msg = zk_common.deserialize_msg(raw_message)
                callback(msg)
            except Exception:
                LOG.exception(_("Failed to process message..."))
            else:
                self.locking_queue.consume()

        data = self.locking_queue.get(timeout=0.1)
        if data is not None:
            _callback(data)

    def cancel(self):
        """Cancel the consuming from the queue, if it has started."""
        try:
            self.queue.cancel(self.tag)
        except KeyError, e:
            if str(e) != "u'%s'" % self.tag:
                raise
        self.queue = None


class QueueConsumer(ConsumerBase):
    """Consumer class for 'queue'."""

    def __init__(self, conf, queue, callback, tag, **kwargs):
        super(QueueConsumer, self).__init__(queue,
                                            callback,
                                            kwargs)


class Connection(object):
    """Connection object."""

    pool = None

    def __init__(self, conf, server_params=None):
        self.consumers = []
        self.consumer_thread = None
        self.proxy_callbacks = []
        self.conf = conf
        self.max_retries = self.conf.zookeeper.kazoo_max_retries
        # Try forever?
        if self.max_retries <= 0:
            self.max_retries = None
        self.interval_start = self.conf.zookeeper.kazoo_retry_interval
        self.interval_stepping = self.conf.zookeeper.kazoo_retry_backoff
        # max retry-interval = 30 seconds
        self.interval_max = 30
        self.memory_transport = False

        if server_params is None:
            server_params = {}
        # Keys to translate from server_params to kazoo params
        server_params_to_kazoo_params = {'username': 'userid'}

        params_list = []
        for adr in self.conf.zookeeper.zk_hosts.split(','):
            hostname, port = network_utils.parse_host_port(
                adr, default_port=self.conf.zookeeper.zk_port)

            params = {
                'hostname': hostname,
                'port': port,
            }

            for sp_key, value in server_params.iteritems():
                p_key = server_params_to_kazoo_params.get(sp_key, sp_key)
                params[p_key] = value

            params_list.append(params)

        self.params_list = params_list

        self.connection = None
        self.reconnect()

    def _connect(self, params):
        """Connect to zookeeper.  Re-establish any queues that may have
        been declared before if we are reconnecting.  Exceptions should
        be handled by the caller.
        """
        if self.connection:
            LOG.info(_("Reconnecting to Zookeeper server on "
                     "%(hostname)s:%(port)d") % params)
            try:
                self.connection.stop()
            except Exception:
                pass
            # Setting this in case the next statement fails, though
            # it shouldn't be doing any network operations, yet.
            self.connection = None
        self.connection = kazoo.client.KazooClient(CONF.zookeeper.zk_hosts)
        self.consumer_num = itertools.count(1)
        try:
            self.connection.start()
            LOG.info(_('Connected to Zookeeper server on '
                       '%(hostname)s:%(port)d') % params)
        except threading.TimeoutError:
            raise zk_common.ConnectionTimeoutError

    def reconnect(self):
        """Handles reconnecting and re-establishing queues.
        Will retry up to self.max_retries number of times.
        self.max_retries = 0 means to retry forever.
        Sleep between tries, starting at self.interval_start
        seconds, backing off self.interval_stepping number of seconds
        each attempt.
        """
        attempt = 0
        while True:
            params = self.params_list[attempt % len(self.params_list)]
            attempt += 1
            try:
                self._connect(params)
                return
            except (IOError) as e:
                pass
            except Exception, e:
                if 'timeout' not in str(e):
                    raise

            log_info = {}
            log_info['err_str'] = str(e)
            log_info['max_retries'] = self.max_retries
            log_info.update(params)

            if self.max_retries and attempt == self.max_retries:
                LOG.error(_('Unable to connect to Zookeeper server on '
                            '%(hostname)s:%(port)d after %(max_retries)d '
                            'tries: %(err_str)s') % log_info)
                sys.exit(1)

            if attempt == 1:
                sleep_time = self.interval_start or 1
            elif attempt > 1:
                sleep_time += self.interval_stepping
            if self.interval_max:
                sleep_time = min(sleep_time, self.interval_max)

            log_info['sleep_time'] = sleep_time
            LOG.error(_('Zookeeper server on %(hostname)s:%(port)d is '
                        'unreachable: %(err_str)s. Trying again in '
                        '%(sleep_time)d seconds.') % log_info)
            time.sleep(sleep_time)

    def ensure(self, error_callback, method, *args, **kwargs):
        while True:
            try:
                return method(*args, **kwargs)
            except exceptions.ConnectionLoss:
                self.reconnect()
            except Exception, e:
                if error_callback:
                    error_callback(e)

    def close(self):
        """Close/release this connection."""
        self.cancel_consumer_thread()
        self.wait_on_proxy_callbacks()
        self.connection.stop()
        self.connection = None

    def reset(self):
        """Reset a connection so it can be used again."""
        self.cancel_consumer_thread()
        self.wait_on_proxy_callbacks()
        self.consumers = []

    def declare_consumer(self, consumer_cls, queue, callback):
        """Create a Consumer using the class that was passed in and
        add it to our list of consumers.
        """

        def _connect_error(exc):
            log_info = {'queue': queue, 'err_str': str(exc)}
            LOG.error(_("Failed to declare consumer for queue '%(queue)s': "
                      "%(err_str)s") % log_info)

        def _declare_consumer():
            consumer = consumer_cls(self.conf, queue, callback,
                                    self.consumer_num.next())
            self.consumers.append(consumer)
            return consumer

        return self.ensure(_connect_error, _declare_consumer)

    def iterconsume(self, limit=None, timeout=None):
        """Return an iterator that will consume from all queues/consumers."""

        info = {'do_consume': True}

        def _error_callback(exc):
            if isinstance(exc, socket.timeout):
                LOG.debug(_('Timed out waiting for RPC response: %s') %
                          str(exc))
                raise zk_common.Timeout()
            else:
                LOG.exception(_('Failed to consume message from queue: %s') %
                              str(exc))
                info['do_consume'] = True

        def _consume():
            if info['do_consume']:
                for queue in self.consumers:
                    queue.consume(nowait=True)
                #info['do_consume'] = False

        for iteration in itertools.count(0):
            if limit and iteration >= limit:
                raise StopIteration
            yield self.ensure(_error_callback, _consume)

    def cancel_consumer_thread(self):
        """Cancel a consumer thread."""
        if self.consumer_thread is not None:
            self.consumer_thread.kill()
            try:
                self.consumer_thread.wait()
            except greenlet.GreenletExit:
                pass
            self.consumer_thread = None

    def wait_on_proxy_callbacks(self):
        """Wait for all proxy callback threads to exit."""
        for proxy_cb in self.proxy_callbacks:
            proxy_cb.wait()

    def publisher_send(self, cls, locking_queue, msg, **kwargs):
        """Send to a publisher based on the publisher class."""

        def _error_callback(exc):
            log_info = {'queue': locking_queue, 'err_str': str(exc)}
            LOG.exception(_("Failed to publish message to queue"
                          "'%(queue)s': %(err_str)s") % log_info)

        def _publish():
            publisher = cls(self.conf, locking_queue)
            publisher.send(msg, priority=100)

        self.ensure(_error_callback, _publish)

    def declare_queue_consumer(self, locking_queue, callback=None):
        """Create a 'queue' consumer."""
        self.declare_consumer(functools.partial(QueueConsumer),
                              locking_queue, callback)

    def put_message_in_queue(self, context, queue, msg, timeout=None):
        """Send a 'queue' message."""
        msg = json.dumps(msg)
        locking_queue = self.connection.LockingQueue(queue)
        self.publisher_send(QueuePublisher, locking_queue,
                            msg, timeout=timeout)

    def create_node(self, path, data):
        """Send a 'queue' message."""
        data = json.dumps(data)

        def _error_callback(exc):
            log_info = {'path': path, 'err_str': str(exc)}
            LOG.exception(_("Failed to create node at path"
                          "'%(path)s': %(err_str)s") % log_info)

        def _create_node():
            self.connection.create(path, data, makepath=True, ephemeral=False)

        self.ensure(_error_callback, _create_node)

    def consume(self, limit=None):
        """Consume from all queues/consumers."""
        it = self.iterconsume(limit=limit)
        while True:
            try:
                it.next()
            except StopIteration:
                return

    def consume_in_thread(self):
        """Consumer from all queues/consumers in a greenthread."""
        def _consumer_thread():
            try:
                self.consume()
            except greenlet.GreenletExit:
                return
        if self.consumer_thread is None:
            self.consumer_thread = eventlet.spawn(_consumer_thread)
        return self.consumer_thread

    def create_consumer(self, queue, proxy):
        """Create a consumer that calls a method in a proxy object."""
        proxy_cb = zk_client.ProxyCallback(
            self.conf, proxy,
            zk_client.get_connection_pool(self.conf, Connection))

        self.proxy_callbacks.append(proxy_cb)
        locking_queue = self.connection.LockingQueue(queue)
        self.declare_queue_consumer(locking_queue, proxy_cb)

    def get_children(self, path):
        """Get the children of the given path."""
        def _error_callback(exc):
            log_info = {'path': path, 'err_str': str(exc)}
            LOG.exception(_("Could not get the child nodes from path"
                          "'%(path)s': %(err_str)s") % log_info)

        def _get_children():
            return self.connection.get_children(path)

        return self.ensure(_error_callback, _get_children)

    def get_data(self, path):
        """Get the data from the given path/node."""
        def _error_callback(exc):
            log_info = {'path': path, 'err_str': str(exc)}
            LOG.exception(_("Could not get value from path"
                          "'%(path)s': %(err_str)s") % log_info)

        def _get():
            return self.connection.get(path)

        self.ensure(_error_callback, _get)

    def set_data(self, path, data):
        data = json.dumps(data)
        """Set the value of a node"""
        def _error_callback(exc):
            log_info = {'path': path, 'err_str': str(exc)}
            LOG.exception(_("Could not set the value to the path"
                          "'%(path)s': %(err_str)s") % log_info)

        def _set():
            #msg = json.dumps(data)
            return self.connection.set(path, data)

        self.ensure(_error_callback, _set)


def create_connection(conf, new=True):
    """Create a connection."""
    return zk_client.create_connection(conf, new,
                              zk_client.get_connection_pool(conf, Connection))


def put_message_in_queue(conf, context, queue, msg):
    """Sends a message on a queue without waiting for a response."""
    return zk_client.put_message_in_queue(
        conf, context, queue, msg,
        zk_client.get_connection_pool(conf, Connection))


def create_node(conf, context, path, data):
    """Sends a message on a queue without waiting for a response."""
    return zk_client.create_node(conf, context, path, data,
                            zk_client.get_connection_pool(conf, Connection))


def get_children(conf, context, path):
    """Get the children of the given node."""
    return zk_client.get_children(conf, context, path,
                                  zk_client.get_connection_pool(conf,
                                                                Connection))


def get_data(conf, context, path):
    """Get the data from the given node/path."""
    return zk_client.get_data(conf, context, path,
                            zk_client.get_connection_pool(conf, Connection))


def set_data(conf, context, path, data):
    """set the data of a node."""
    return zk_client.set_data(conf, context, path, data,
                            zk_client.get_connection_pool(conf, Connection))
