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

import copy

from kazoo import exceptions

from oslo.config import cfg

from nova.openstack.common import jsonutils
from nova.openstack.common import local
from nova.openstack.common import log as logging


CONF = cfg.CONF
LOG = logging.getLogger(__name__)

_ZK_ENVELOPE_VERSION = '1.0'

_VERSION_KEY = 'orc.version'
_MESSAGE_KEY = 'orc.message'


_SEND_ZK_ENVELOPE = False


class ZKException(Exception):
    message = _("An unknown ZK related exception occurred.")

    def __init__(self, message=None, **kwargs):
        self.kwargs = kwargs

        if not message:
            try:
                message = self.message % kwargs

            except Exception:
                # kwargs doesn't match a variable in the message
                # log the issue and the kwargs
                LOG.exception(_('Exception in string format operation'))
                for name, value in kwargs.iteritems():
                    LOG.error("%s: %s" % (name, value))
                # at least get the core message out if something happened
                message = self.message

        super(ZKException, self).__init__(message)


class RemoteError(ZKException):
    """Signifies that a remote class has raised an exception.

    Contains a string representation of the type of the original exception,
    the value of the original exception, and the traceback.  These are
    sent to the parent as a joined string so printing the exception
    contains all of the relevant info.

    """
    message = _("Remote error: %(exc_type)s %(value)s\n%(traceback)s.")

    def __init__(self, exc_type=None, value=None, traceback=None):
        self.exc_type = exc_type
        self.value = value
        self.traceback = traceback
        super(RemoteError, self).__init__(exc_type=exc_type,
                                          value=value,
                                          traceback=traceback)


class Timeout(exceptions.SessionExpiredError):
    """Signifies that a timeout has occurred.

    This exception is raised if the rpc_response_timeout is reached while
    waiting for a response from the remote side.
    """
    message = _("Timed out while waiting on ZooKeeper response.")


class DuplicateMessageError(ZKException):
    message = _("Found duplicate message(%(msg_id)s). Skipping it.")


class InvalidZKConnectionReuse(ZKException):
    message = _("Invalid reuse of an RPC connection.")


class UnsupportedZkEnvelopeVersion(ZKException):
    message = _("Specified Zk envelope version, %(version)s, "
                "not supported by this endpoint.")


class ConnectionTimeoutError(ZKException):
    message = _("Connection Timed-out")


class Connection(object):
    """A connection, returned by rpc.create_connection().

    This class represents a connection to the message bus used for rpc.
    An instance of this class should never be created by users of the rpc API.
    Use rpc.create_connection() instead.
    """
    def close(self):
        """Close the connection.

        This method must be called when the connection will no longer be used.
        It will ensure that any resources associated with the connection, such
        as a network connection, and cleaned up.
        """
        raise NotImplementedError()

    def create_consumer(self, queue, proxy):
        """Create a consumer on this connection.

        A consumer is associated with a message queue on the backend message
        bus.  The consumer will read messages from the queue, unpack them, and
        dispatch them to the proxy object.  The contents of the message pulled
        off of the queue will determine which method gets called on the proxy
        object.

        :param topic: This is a name associated with what to consume from.
                      Multiple instances of a service may consume from the same
                      topic. For example, all instances of nova-compute consume
                      from a queue called "compute".  In that case, the
                      messages will get distributed amongst the consumers in a
                      round-robin fashion if fanout=False.  If fanout=True,
                      every consumer associated with this topic will get a
                      copy of every message.
        :param proxy: The object that will handle all incoming messages.
        """
        raise NotImplementedError()

    def consume_in_thread(self):
        """Spawn a thread to handle incoming messages.

        Spawn a thread that will be responsible for handling all incoming
        messages for consumers that were set up on this connection.

        Message dispatching inside of this is expected to be implemented in a
        non-blocking manner.  An example implementation would be having this
        thread pull messages in for all of the consumers, but utilize a thread
        pool for dispatching the messages to the proxy objects.
        """
        raise NotImplementedError()


class CommonZkContext(object):
    def __init__(self, **kwargs):
        self.values = kwargs

    def __getattr__(self, key):
        try:
            return self.values[key]
        except KeyError:
            raise AttributeError(key)

    def to_dict(self):
        return copy.deepcopy(self.values)

    @classmethod
    def from_dict(cls, values):
        return cls(**values)

    def deepcopy(self):
        return self.from_dict(self.to_dict())

    def update_store(self):
        local.store.context = self

    def elevated(self, read_deleted=None, overwrite=False):
        """Return a version of this context with admin flag set."""
        # TODO(russellb) This method is a bit of a nova-ism.  It makes
        # some assumptions about the data in the request context sent
        # across rpc, while the rest of this class does not.  We could get
        # rid of this if we changed the nova code that uses this to
        # convert the RpcContext back to its native RequestContext doing
        # something like nova.context.RequestContext.from_dict(ctxt.to_dict())

        context = self.deepcopy()
        context.values['is_admin'] = True

        context.values.setdefault('roles', [])

        if 'admin' not in context.values['roles']:
            context.values['roles'].append('admin')

        if read_deleted is not None:
            context.values['read_deleted'] = read_deleted

        return context


def _safe_log(log_func, msg, msg_data):
    """Sanitizes the msg_data field before logging."""
    SANITIZE = {'set_admin_password': [('args', 'new_pass')],
                'run_instance': [('args', 'admin_password')],
                'route_message': [('args', 'message', 'args', 'method_info',
                                   'method_kwargs', 'password'),
                                  ('args', 'message', 'args', 'method_info',
                                   'method_kwargs', 'admin_password')]}

    has_method = 'method' in msg_data and msg_data['method'] in SANITIZE
    has_context_token = '_context_auth_token' in msg_data
    has_token = 'auth_token' in msg_data

    if not any([has_method, has_context_token, has_token]):
        return log_func(msg, msg_data)

    msg_data = copy.deepcopy(msg_data)

    if has_method:
        for arg in SANITIZE.get(msg_data['method'], []):
            try:
                d = msg_data
                for elem in arg[:-1]:
                    d = d[elem]
                d[arg[-1]] = '<SANITIZED>'
            except KeyError, e:
                LOG.info(_('Failed to sanitize %(item)s. Key error %(err)s'),
                         {'item': arg,
                          'err': e})

    if has_context_token:
        msg_data['_context_auth_token'] = '<SANITIZED>'

    if has_token:
        msg_data['auth_token'] = '<SANITIZED>'

    return log_func(msg, msg_data)


def version_is_compatible(imp_version, version):
    """Determine whether versions are compatible.

    :param imp_version: The version implemented
    :param version: The version requested by an incoming message.
    """
    version_parts = version.split('.')
    imp_version_parts = imp_version.split('.')
    if int(version_parts[0]) != int(imp_version_parts[0]):  # Major
        return False
    if int(version_parts[1]) > int(imp_version_parts[1]):  # Minor
        return False
    return True


def serialize_msg(raw_msg, force_envelope=False):
    if not _SEND_ZK_ENVELOPE and not force_envelope:
        return raw_msg

    # NOTE(russellb) See the docstring for _RPC_ENVELOPE_VERSION for more
    # information about this format.
    msg = {_VERSION_KEY: _ZK_ENVELOPE_VERSION,
           _MESSAGE_KEY: jsonutils.dumps(raw_msg)}

    return msg


def deserialize_msg(msg):
    # NOTE(russellb): Hang on to your hats, this road is about to
    # get a little bumpy.
    #
    # Robustness Principle:
    #    "Be strict in what you send, liberal in what you accept."
    #
    # At this point we have to do a bit of guessing about what it
    # is we just received.  Here is the set of possibilities:
    #
    # 1) We received a dict.  This could be 2 things:
    #
    #   a) Inspect it to see if it looks like a standard message envelope.
    #      If so, great!
    #
    #   b) If it doesn't look like a standard message envelope, it could either
    #      be a notification, or a message from before we added a message
    #      envelope (referred to as version 1.0).
    #      Just return the message as-is.
    #
    # 2) It's any other non-dict type.  Just return it and hope for the best.
    #    This case covers return values from rpc.call() from before message
    #    envelopes were used.  (messages to call a method were always a dict)

    if not isinstance(msg, dict):
        # See #2 above.
        return msg

    base_envelope_keys = (_VERSION_KEY, _MESSAGE_KEY)
    if not all(map(lambda key: key in msg, base_envelope_keys)):
        #  See #1.b above.
        return msg

    # At this point we think we have the message envelope
    # format we were expecting. (#1.a above)

    if not version_is_compatible(_ZK_ENVELOPE_VERSION, msg[_VERSION_KEY]):
        raise UnsupportedZkEnvelopeVersion(version=msg[_VERSION_KEY])

    raw_msg = jsonutils.loads(msg[_MESSAGE_KEY])

    return raw_msg
