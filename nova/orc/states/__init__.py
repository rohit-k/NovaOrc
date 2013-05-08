# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import abc
import copy

try:
    import collections.OrderedDict as OrderedDict
except ImportError:
    import ordereddict.OrderedDict as OrderedDict

from nova.db import base
from nova import exception as excp
from nova.openstack.common import excutils
from nova.openstack.common import log as logging

LOG = logging.getLogger(__name__)

# Useful to know when other states are being activated and finishing.
STARTING = 'STARTING'
COMPLETED = 'COMPLETED'
ERRORED = 'ERRORED'


class StateChain(object):
    def __init__(self, name, tolerant=False, parents=None):
        self.reversions = []
        self.name = name
        self.tolerant = tolerant
        self.states = OrderedDict()
        self.results = OrderedDict()
        self.parents = parents
        self.result_fetcher = None
        self.change_tracker = None
        self.listeners = []

    def __setitem__(self, name, performer):
        self.states[name] = performer

    def __getitem__(self, name):
        return self.results[name]

    def run(self, context, *args, **kwargs):
        for (name, performer) in self.states.items():
            try:
                self._on_state_start(context, performer, name)
                # See if we have already ran this...
                result = None
                if self.result_fetcher:
                    result = self.result_fetcher(context, name, self)
                if result is None:
                    result = performer.apply(context, *args, **kwargs)
                # Keep a pristine copy of the result in the results table
                # so that if said result is altered by other further states
                # the one here will not be.
                self.results[name] = copy.deepcopy(result)
                self._on_state_finish(context, performer, name, result)
            except Exception as ex:
                with excutils.save_and_reraise_exception():
                    try:
                        self._on_state_error(context, name, ex)
                    except Exception:
                        pass
                    cause = (name, performer, (args, kwargs))
                    self.rollback(context, name, self, ex, cause)
        return self

    def _on_state_error(self, context, name, ex):
        if self.change_tracker:
            self.change_tracker(context, ERRORED, name, self)
        for i in self.listeners:
            i.notify(context, ERRORED, name, self, error=ex)

    def _on_state_start(self, context, performer, name):
        if self.change_tracker:
            self.change_tracker(context, STARTING, name, self)
        for i in self.listeners:
            i.notify(context, STARTING, name, self)

    def _on_state_finish(self, context, performer, name, result):
        # If a future state fails we need to ensure that we
        # revert the one we just finished.
        self.reversions.append((name, performer))
        if self.change_tracker:
            self.change_tracker(context, COMPLETED, name, self,
                                result=result.to_dict())
        for i in self.listeners:
            i.notify(context, COMPLETED, name, self, result=result)

    def rollback(self, context, name, chain=None, ex=None, cause=None):
        if chain is None:
            chain = self
        for (i, (name, performer)) in enumerate(reversed(self.reversions)):
            try:
                performer.revert(context, self.results[name], chain, ex, cause)
            except excp.NovaException:
                # Ex: WARN: Failed rolling back stage 1 (validate_request) of
                #           chain validation due to nova exception
                # WARN: Failed rolling back stage 2 (create_db_entry) of
                #       chain init_db_entry due to nova exception
                msg = _("Failed rolling back stage %(stage)s (%(performer)s)"
                        " of chain %(name)s due to nova exception.")
                LOG.warn(msg, {'stage': (i + 1), 'performer': performer.name,
                               'name': self.name})
                if not self.tolerant:
                    # This will log a msg AND re-raise the Nova exception if
                    # the chain does not tolerate exceptions
                    raise
            except Exception:
                # Ex: WARN: Failed rolling back stage 1 (validate_request) of
                #           chain validation due to unknown exception
                #     WARN: Failed rolling back stage 2 (create_db_entry) of
                #           chain init_db_entry due to unknown exception
                msg = _("Failed rolling back stage %(stage)s (%(performer)s)"
                        " of chain %(name)s due to nova exception.")
                LOG.warn(msg, {'stage': (i + 1), 'performer': performer.name,
                               'name': self.name})
                if not self.tolerant:
                    # Log a msg AND re-raise the generic Exception if the
                    # Chain does not tolerate exceptions
                    raise
        if self.parents:
            # Rollback any parents chains
            for p in self.parents:
                p.rollback(context, name, chain, ex, cause)


class State(base.Base):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        super(State, self).__init__()

    def __str__(self):
        return "State: %s" % (self.__class__.__name__)

    @abc.abstractmethod
    def apply(self, context, *args, **kwargs):
        raise NotImplementedError()

    def revert(self, context, result, chain, excp, cause):
        pass


class ResourceUsingState(State):
    def __init__(self, **kwargs):
        super(ResourceUsingState, self).__init__()
        self.name = self.__class__.__name__
        self.compute_rpcapi = kwargs.get("compute_rpcapi")
        self.conductor_api = kwargs.get("conductor_api")
        self.network_api = kwargs.get("network_api")
        self.scheduler_rpcapi = kwargs.get("scheduler_rpcapi")
        self.volume_api = kwargs.get("volume_api")
