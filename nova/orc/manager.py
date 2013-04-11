# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
# Copyright (c) 2013 NTT Data. All Rights Reserved.
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

from oslo.config import cfg

from nova.compute import rpcapi as compute_rpcapi
from nova import conductor
from nova.db import base
from nova import manager
from nova import network
from nova.openstack.common import importutils
from nova.openstack.common import jsonutils
from nova.openstack.common import log as logging
from nova.openstack.common.notifier import api as notifier
from nova.orc import rpcapi as orc_rpcapi
from nova.orc import states
from nova.orc.resources import compute as compute_resource
from nova.orc.states import compute as cs
from nova import utils
from nova.scheduler import rpcapi as scheduler_rpcapi
from nova import volume

LOG = logging.getLogger(__name__)


reservation_driver_opts = [
    cfg.StrOpt('reserve_instances_driver',
               default='nova.orc.states.plugins.reserve_instances.'
               'ReserveInstancesDriver',
               help='Default driver for reserving instances'),
    cfg.StrOpt('reserve_networks_driver',
               default='nova.orc.states.plugins.reserve_networks.'
               'ReserveNetworksDriver',
               help='Default driver for reserving networks'),
    cfg.StrOpt('reserve_volumes_driver',
               default='nova.orc.states.plugins.reserve_volumes.'
               'ReserveVolumesDriver',
               help='Default driver for reserving volumes'),
]

provisioning_driver_opts = [
    cfg.StrOpt('provision_instances_driver',
        default='nova.orc.states.plugins.provision_instances.'
                'ProvisionInstancesDriver',
        help='Default driver for provisioning instances'),
    cfg.StrOpt('provision_networks_driver',
        default='nova.orc.states.plugins.provision_networks.'
                'ProvisionNetworksDriver',
        help='Default driver for provisioning networks'),
    cfg.StrOpt('provision_volumes_driver',
        default='nova.orc.states.plugins.provision_volumes.'
                'ProvisionVolumesDriver',
        help='Default driver for provisioning volumes'),
]


CONF = cfg.CONF
CONF.register_opts(reservation_driver_opts, group='orchestration')
CONF.register_opts(provisioning_driver_opts, group='orchestration')


def _get_chain_state_name(chain, state_name, sep=":"):
    return "%s%s%s" % (chain.name, sep, state_name)


def _make_dynamic_state(driver_name, **kwargs):
    return importutils.import_object(driver_name, **kwargs)


class ResourceTrackingWorkflow(object):
    def __init__(self, db, context):
        self.db = db
        self.context = context
        self.admin_context = context.elevated()

    def _get_history(self, resource):
        # TODO(harlowja): get the list of workflows + states already performed
        # on this resource...
        actions = self.db.resource_tracker_actions_get(self.admin_context,
                                                       resource.tracking_id)
        LOG.debug(_("Getting the state history of %s"), resource.tracking_id)
        LOG.debug(_("The state history is %s"), actions)
        return actions

    def _initialize_resource(self, resource):
        if resource.tracking_id:
            return
        # Have to start tracking this resource so that we can know
        # in the future what its tracking 'id' is so that we can
        # reference its past work and resume from said previous states
        # if needbe.
        resource.tracking_id = utils.generate_uid('r')
        what_started = {
            'request_uuid': self.admin_context.request_id,
            'tracking_id': resource.tracking_id,
            'status': states.STARTING,
        }
        self.db.resource_tracker_create(self.admin_context, what_started)
        LOG.debug(_("Starting to track request id %s fullfillment"
                    " using tracking id %s"),
                  self.admin_context.request_id, resource.tracking_id)

    def run(self, chains, resource, *args, **kwargs):
        self._initialize_resource(resource)
        resource_history = self._get_history(resource)

        def change_tracker(context, state, state_name, chain, **kwargs):
            full_name = _get_chain_state_name(chain, state_name)
            if state == states.COMPLETED:
                result = kwargs.get('result')
                resource_history[full_name] = result
                # NOTE(harlowja): save it to the database so that the resource
                # request can be tracked as to what is occuring to the resource
                # as it moves between states, and also save the result of that
                # state change so that if later resumed that it can be resumed
                # by a future 'orc' and possibly rolled-back by that future
                # orc as well...
                what_changed = {
                    'tracking_id': resource.tracking_id,
                    'action_performed': full_name,
                    'action_result': jsonutils.dumps(result),
                }
                self.db.resource_tracker_action_create(self.admin_context,
                                                       what_changed)
                LOG.debug(_("Saving that %(tracking_id)s completed state"
                            " %(action_performed)s with %(action_result)s"),
                          what_changed)
            elif state == states.STARTING:
                LOG.debug(_("Starting state %s"), full_name)
            elif state == states.ERRORED:
                LOG.debug(_("Errored out during state %s"), full_name)

        def result_fetcher(context, state_name, chain):
            full_name = _get_chain_state_name(chain, state_name)
            if full_name in resource_history:
                # NOTE(harlowja): Already have ran this flow + state,
                # so lets send back its previous results (so that
                # rollback can work correctly)
                return resource_history[full_name]
            else:
                return None

        for c in chains:
            # NOTE(harlowja): Let us be able to notify others when states
            # error/start/stop using this attached listener.
            if self not in c.listeners:
                c.listeners.append(self)
            # NOTE(harlowja): this one is used so that we can let the chain
            # running algorithm know that we have already completed said state
            # previously (so that it can skip).
            c.result_fetcher = result_fetcher
            # NOTE(harlowja): Allow us to persist completion events and there
            # results to a persistant storage, so that we can keep a history
            # of the states that have occurred (useful for resumption).
            c.change_tracker = change_tracker
            c.run(self.context, resource, *args, **kwargs)

    def notify(self, context, status, state_name, chain, error=None, result=None):
        event_type = 'orc_%s' % _get_chain_state_name(chain.name, state_name,
                                                      sep=".")
        payload = dict(status=status, result=result, error=error)
        if status == states.ERRORED:
            notifier.notify(context, notifier.publisher_id("orc"), event_type,
                            notifier.ERROR, payload)
        else:
            notifier.notify(context, notifier.publisher_id("orc"), event_type,
                            notifier.INFO, payload)


class OrchestrationManager(manager.Manager):
    """Orchestrates a given request, handling state transitions
       as well as rollbacks that may or may not be triggered
       by successful calls or unsuccessful ones."""

    def __init__(self, *args, **kwargs):
        self.orc_rpcapi = orc_rpcapi.OrchestrationAPI()
        self.compute_rpcapi = compute_rpcapi.ComputeAPI()
        self.scheduler_rpcapi = scheduler_rpcapi.SchedulerAPI()
        self.network_api = network.API()
        self.volume_api = volume.API()
        self.conductor_api = conductor.API()

        # TODO: this should likely not be here, but instead should be
        # using the conductor api??
        db_base = base.Base()
        self.db = db_base.db

        super(OrchestrationManager, self).__init__(*args, **kwargs)

    # Provides a high level result oriented api while hiding all the
    # inner complexity of fulfilling a request internally via a state
    # like tracking mechanism (that will start of being very simple
    # with future improvements to make it much more complex).

    def _create_resource(self, **kwargs):
        """Return an instance of a Compute resource object"""
        return compute_resource.Create(**kwargs)

    def fulfill_compute_create(self, context, **kwargs):
        # The following set of stages are compose a compute resource
        # fulfillment and how those stages will be unrolled shall
        # reported on and errors stored must be described here.
        #
        # 0. Validate resource components and resource policies for
        #    a basic level of sanity (including items like quota
        #    checking, metadata properties, requested networks, image
        #    status and ensure image matches desired capabilities,
        #    userdata size and so on...
        #
        #    Error:
        #      a. On failure of any validation step raise an
        #         appropriate exception to the caller and abort the task
        #
        # 1. Ensure database entry with current resource to-be
        #    in place and place resource into the 'validating' task state.
        #
        #    Error:
        #       a. On database entry adjustment failure nothing has
        #          been performed and raise an appropriate exception to
        #          the caller
        #
        # 2. Validate resource consumption points such as image id existing
        #    and currently accessible, said network that you have specified
        #    (if any) is currently accessible and so on. Note that even those
        #    these pre-checks occur it is not possible until later to ensure
        #    that said further resource allocations will continue to exist but
        #    checking that they exist now instead of waiting until then allows
        #    the orchestration entity to bail out earlier rather than later...
        #
        #    Error:
        #      a. Set the database task state to 'failed-validation' and throw
        #         an appropriate exception to notify the caller that the
        #         validation has failed and what piece of validation did not
        #         pass and why.
        #      b. TODO(harlowja): what to do about database entry 'trash'?
        #
        # 3. Begin formation of fulfilled request "document"
        # 4. Communicate with *compute* scheduler asking it where should
        #    the target compute hypervisors for said resource/s be placed
        #
        #    Error:
        #      a. On failure to communicate with the scheduler service via
        #         a blocking RPC call or a timeout waiting for said response
        #         to arrive mark database instance as 'failed-scheduling'
        #         and throw an appropriate exception to notify the caller
        #      b. On partial failure (where a segment of the desired nodes
        #         are selected) retry calling into the scheduler to fill the
        #         missing amount of vm's. On failure to accomplish this call
        #         into the scheduler and free the currently provisioned
        #         resources that were handed out in the first request and
        #         follow step 'a'
        #
        # First validate the resource request as much as we can up-front
        # for a level of sanity that we will accept before we start mucking
        # around with actually fulfilling said resource request....

        # This translates the kwargs into a object which has nice fields
        # that can be accessed and provides other useful methods on said
        # fields...
        resource = self._create_resource(**kwargs)

        # Define the validation state chain and it's performers
        validate = states.StateChain('validation')
        validate['policies'] = cs.ValidateResourcePolicies()
        validate['image'] = cs.ValidateResourceImage()
        validate['request'] = cs.ValidateResourceRequest()

        # From here on out we start collecting all rollbacks into a single
        # list of chains since we are now affecting external state and
        # all post-validation states must be given the chance to rollback
        db_entry = states.StateChain('initial_db_entry')
        db_entry['compute_create'] = cs.CreateComputeEntry()

        # Now run this initial workflow
        activator = ResourceTrackingWorkflow(self.db, context)
        activator.run([validate, db_entry], resource)

        # Proxy to the real orc-api to get the rest of the work done...
        self.orc_rpcapi.reserve_and_provision_resources(context,
                                                        resource.to_dict())

        return (resource.instances, resource.tracking_id)

    def reserve_and_provision_resources(self, context, resource):
        # Ok we should now be at a level of request sanity where we can
        # actually start performing said fulfillment without knowing that it
        # will fail immediately. It could though still fail due to the async
        # nature that this software runs.
        resource = self._create_resource(**resource)

        # Arguments passed to all dynamic state creates...
        make_state_args = {
            'scheduler_rpcapi': self.scheduler_rpcapi,
            'conductor_api': self.conductor_api,
            'volume_api': self.volume_api,
            'network_api': self.network_api,
            'compute_rpcapi': self.compute_rpcapi,
        }

        # Start filling in the parts of the final provision document
        # 'ask' and instead of forwarding it along. This is going to require
        # some scheduling changes that are very beneficial in the long term
        # as we start to need to connect all 'resource' requests into a single
        # document that is finally sent to the target compute node set. This
        # allows this entity to 'smartly' make those kind of combined placement
        # decisions instead of having to send 'hints' around that are
        # suboptimal and don't scale correctly...
        reserve = states.StateChain('reservation')
        reserve_instances_driver = CONF.orchestration.reserve_instances_driver
        reserve['instances'] = _make_dynamic_state(reserve_instances_driver,
                                                   **make_state_args)
        reserve_networks_driver = CONF.orchestration.reserve_networks_driver
        reserve['networks'] = _make_dynamic_state(reserve_networks_driver,
                                                  **make_state_args)
        reserve_volumes_driver = CONF.orchestration.reserve_volumes_driver
        reserve['volumes'] = _make_dynamic_state(reserve_volumes_driver,
                                                 **make_state_args)
        activator = ResourceTrackingWorkflow(self.db, context)
        activator.run([reserve], resource)

        # Now start to form the document that will say what is provisioned..
        provision_doc = compute_resource.MultiProvisionDocument()
        provision_doc.networks = reserve.results['networks']
        provision_doc.volumes = reserve.results['volumes']
        provision_doc.instances = reserve.results['instances']

        # And finally...
        #
        # (1) Finally we can now fire off said document to each target
        #     hypervisor asking said hypervisor to 'create' a vm but not setup
        #     said vm's network or volumes.
        #
        # (2) Transition target hypervisors from 'creation' state to 'prepared'
        #     state where each hypervisor now prepares instance to be activated
        #     by establish network and volume and vm resources on the
        #     local hypervisor.
        #
        # (3) Transition target hypervisors from 'prepared' state to 'active'
        #     state where each hypervisor now activates requested instances.
        #
        #     At the end of each stage the orc unit should then wait for an
        #     'ack' from each targeted hypervisor, and upon failure to receive
        #     said 'ack' decide it needs to decide how to best handle recovery
        #     of those instances.
        provision = states.StateChain('provisioning', parents=[reserve])
        provision_networks_driver = \
                                  CONF.orchestration.provision_networks_driver
        provision['networks'] = _make_dynamic_state(provision_networks_driver,
                                                    **make_state_args)
        provision_volumes_driver = CONF.orchestration.provision_volumes_driver
        provision['volumes'] = _make_dynamic_state(provision_volumes_driver,
                                                   **make_state_args)
        provision_instances_driver = \
                                 CONF.orchestration.provision_instances_driver
        provision['instances'] = _make_dynamic_state(
                                                    provision_instances_driver,
                                                    **make_state_args)
        activator = ResourceTrackingWorkflow(self.db, context)
        activator.run([provision], resource, provision_doc)

        # Now we need a state to wait for verifification that these instances
        # came up, and if it fails, then we should undo all the other actions
        # done.
        validation = states.StateChain('validation', parents=[provision])
        validation['ensure_booted'] = cs.ValidateBooted(**make_state_args)
        activator = ResourceTrackingWorkflow(self.db, context)
        activator.run([validation], resource, provision_doc)
