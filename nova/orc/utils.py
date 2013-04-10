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


def execute_workflow(function):
    """Decorator that executes a workflow"""
    @functools.wraps(function)
    def decorated_function(self, context, *args, **kwargs):
        workflow_request = kwargs.get("workflow_request")
        wf_type = kwargs.get("workflow_type")
        output = function(self, context, *args, **kwargs)
        #set the current workflow to the next workflow id
        if (workflow_request['current_workflow_id'] == self.WORKFLOW_ID):
            for workflow in wf_type['workflows']:
                if self.WORKFLOW_ID == workflow['id']:
                    next_workflow_id = workflow['next_workflow_id']
                    self.db.workflow_request_update(context.elevated(),
                                                    workflow_request['id'],
                                 {'current_workflow_id': next_workflow_id})
                    workflow_request['current_workflow_id'] = next_workflow_id

        return output
    return decorated_function


class StateChain(object):
    def __init__(self, name, tolerant=False, parents=None):
        self.reversions = []
        self.name = name
        self.tolerant = tolerant
        self.states = OrderedDict()
        self.results = OrderedDict()
        self.parents = parents

    def __setitem__(self, name, performer):
        self.states[name] = performer

    def __getitem__(self, name):
        return self.results[name]

    def run(self, context, *args, **kwargs):
        for (name, performer) in self.states.items():
            try:
                self._on_state_start(context, performer, name)
                result = performer.apply(context, *args, **kwargs)
                self.results[name] = result
                self._on_state_finish(context, performer, name, result)
            except Exception as ex:
                with excutils.save_and_reraise_exception():
                    cause = (name, performer, (args, kwargs))
                    self.rollback(context, name, self, ex, cause)

        return self

    def _on_state_start(self, context, performer, name):
        performer.notify(context, workflow_states.STARTING, name, self)

    def _on_state_finish(self, context, performer, name, result):
        # If a future state fails we need to ensure that we
        # revert the one we just finished.
        LOG.debug("Result of %s:%s chain: %s " % (self.name, name,
                                                  result))
        self.reversions.append(performer)
        performer.notify(context, workflow_states.COMPLETE, name, self,
                         result=result)

    def rollback(self, context, name, chain=None, ex=None, cause=None):
        if chain is None:
            chain = self
        for (i, performer) in enumerate(reversed(self.reversions)):
            try:
                performer.revert(context, chain, ex, cause)
            except excp.NovaException:
                # Ex: WARN: Failed rolling back stage 1 (validate_request) of
                #           chain validation due to nova exception
                # WARN: Failed rolling back stage 2 (create_db_entry) of
                #       chain init_db_entry due to nova exception
                msg = _("Failed rolling back stage %s (%s)"
                        " of chain %s due to nova exception.")
                LOG.warn(msg, (i + 1), performer.name, self.name)
                if not self.tolerant:
                    # Log a msg AND re-raise the Nova exception if the Chain
                    # does not tolerate exceptions
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
