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


import copy
import functools
try:
    from collections import OrderedDict
except IOError:
    from ordereddict import OrderedDict


from nova import exception as excp
from nova.openstack.common import excutils
from nova.openstack.common import log as logging
from nova.orc import states
from nova.orc.states import workflow_states


LOG = logging.getLogger(__name__)


class DictableObject(object):
    def __init__(self, **kwargs):
        if kwargs:
            for (k, v) in kwargs.items():
                setattr(self, k, v)

    def to_dict(self):
        myself = {}
        for (k, v) in self.__dict__.items():
            if k.startswith("_"):
                continue
            myself[k] = v
        return myself

    def __getattr__(self, name):
        # Be tolerant of fields not existing...
        return None


class StateChain(object):
    def __init__(self, name, tolerant=False, parents=None):
        self.reversions = []
        self.name = name
        self.tolerant = tolerant
        self.states = OrderedDict()
        self.results = OrderedDict()
        self.parents = parents
        self.result_fetcher = lambda c, n, s: None
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
                    cause = (name, performer, (args, kwargs))
                    self.rollback(context, name, self, ex, cause)
        return self

    def _on_state_start(self, context, performer, name):
        performer.notify(context, workflow_states.STARTING, name, self)
        for i in self.listeners:
            i.notify(context, workflow_states.STARTING, name, self)

    def _on_state_finish(self, context, performer, name, result):
        # If a future state fails we need to ensure that we
        # revert the one we just finished.
        LOG.debug("Result of %s:%s chain: %s", self.name, name, result)
        # affecting said result later down the line...
        self.reversions.append((name, performer))
        performer.notify(context, workflow_states.COMPLETE, name, self,
                         result=result)
        for i in self.listeners:
            i.notify(context, workflow_states.COMPLETE, name,
                     self, result=result)

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
                msg = _("Failed rolling back stage %s (%s)"
                        " of chain %s due to nova exception.")
                LOG.warn(msg, (i + 1), performer.name, self.name)
                if not self.tolerant:
                    # This will log a msg AND re-raise the Nova exception if
                    # the chain does not tolerate exceptions
                    raise
            except Exception:
                # Ex: WARN: Failed rolling back stage 1 (validate_request) of
                #           chain validation due to unknown exception
                #     WARN: Failed rolling back stage 2 (create_db_entry) of
                #           chain init_db_entry due to unknown exception
                msg = _("Failed rolling back stage %s (%s)"
                        " of chain %s, due to unknown exception.")
                LOG.warn(msg, (i + 1), performer.name, self.name)
                if not self.tolerant:
                    # Log a msg AND re-raise the generic Exception if the
                    # Chain does not tolerate exceptions
                    raise
        if self.parents:
            # Rollback any parents chains
            for p in self.parents:
                p.rollback(context, name, chain, ex, cause)
