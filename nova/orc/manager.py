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


from nova.compute import rpcapi as compute_rpcapi
from nova import conductor
from nova.db import base
from nova import manager
from nova import network
from nova.openstack.common import lockutils
from nova.openstack.common import log as logging
from nova.openstack.common import memorycache
from nova.orc import rpcapi as orc_rpcapi
from nova.orc import utils as o_utils
from nova.orc.resources import compute as compute_resource
from nova.orc.states import compute as cs
from nova.orc.states import workflow_states as wf_states
from nova import utils
from nova.scheduler import rpcapi as scheduler_rpcapi
from nova import volume

LOG = logging.getLogger(__name__)

HYPERVISOR_ACK = 'hypervisor_ack'


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
        db_base = base.Base()
        self.db = db_base.db
        self.mc = memorycache.get_client()

        super(OrchestrationManager, self).__init__(*args, **kwargs)

    # Provides a high level result oriented api while hiding all the
    # inner complexity of fulfilling a request internally via a state
    # like tracking mechanism (that will start of being very simple
    # with future improvements to make it much more complex).

    def _create_resource(self, **kwargs):
        """Return an instance of a Compute resource object"""
        return compute_resource.Create(**kwargs)

    def _create_db_entry_for_workflow_request(self, context,
                                                  workflow_status,
                                                  workflow_type_id,
                                                  workflow_id,
                                                  instance_uuid=None):
        """Create an entry in the db and initiate a new workflow request
        for this state chain performer.
        """
        values = {'status': workflow_status,
                  'workflow_type_id': workflow_type_id,
                  'current_workflow_id': workflow_id,
                  'instance_uuid': instance_uuid}
        elevated = context.elevated()
        workflow_request = self.db.workflow_request_create(elevated, values)

        return workflow_request

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

        resource = self._create_resource(**kwargs)

        # Generate a unique reservation_id for the create request
        if resource.reservation_id is None:
            resource.reservation_id = utils.generate_uid('r')

        # Define the validation state chain and it's performers
        validate = o_utils.StateChain('validation')
        validate['policies'] = cs.ValidateResourcePolicies()
        validate['image'] = cs.ValidateResourceImage()
        validate['request'] = cs.ValidateResourceRequest()
        validate.run(context, resource)

        # From here on out we start collecting all rollbacks into a single
        # list of chains since we are now affecting external state and
        # all post-validation states must be given the chance to rollback
        db_entry = o_utils.StateChain('initial_db_entry')
        db_entry['compute_create'] = cs.CreateComputeEntry()
        db_entry.run(context, resource)

        # Create a workflow request and set the current workflow id of the
        # first performer 'ReserveInstances' of 'CreateServer' workflow type
        workflow_request = self._create_db_entry_for_workflow_request(
                                            context,
                                            wf_states.PENDING,
                                            1,  # Hardcoded 1 for Create Server
                                            cs.ReserveInstances.WORKFLOW_ID)

        resource.workflow_request_id = workflow_request.id
        self.orc_rpcapi.reserve_and_provision_resources(context,
                                                   resource.to_dict())
        return (resource.instances, resource.reservation_id)

    def reserve_and_provision_resources(self, context, resource):
        # Ok we should now be at a level of request sanity where we can
        # actually start performing said fulfillment without knowing that it
        # will fail immediately. It could though still fail due to the async
        # nature that this software runs.

        resource = self._create_resource(**resource)
        workflow_request = self.db.workflow_request_get(context.elevated(),
                                                resource.workflow_request_id)

        wf_type = self.db.workflow_type_get(context.elevated(),
                                          workflow_request.workflow_type_id)

        # Set the workflow status to ACTIVE
        self.db.workflow_request_update(context.elevated(),
                                        workflow_request['id'],
                                        {'status': wf_states.ACTIVE})

        # Start filling in the parts of the final provision document
        # 'ask' and instead of forwarding it along. This is going to require
        # some scheduling changes that are very beneficial in the long term
        # as we start to need to connect all 'resource' requests into a single
        # document that is finally sent to the target compute node set. This
        # allows this entity to 'smartly' make those kind of combined placement
        # decisions instead of having to send 'hints' around that are
        # suboptimal and don't scale correctly...
        reserve = o_utils.StateChain('reservation')
        reserve['instances'] = cs.ReserveInstances(
                                    scheduler_rpcapi=self.scheduler_rpcapi,
                                    conductor_api=self.conductor_api)
        reserve['networks'] = cs.ReserveNetworks(
                                    compute_rpcapi=self.compute_rpcapi,
                                    network_api=self.network_api,
                                    conductor_api=self.conductor_api)
        reserve['volumes'] = cs.ReserveVolumes(
                                    compute_rpcapi=self.compute_rpcapi,
                                    volume_api=self.volume_api,
                                    conductor_api=self.conductor_api)
        reserve.run(context, resource, workflow_request=workflow_request,
                    workflow_type=wf_type)

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

        provision = o_utils.StateChain('provisioning', parents=[reserve])
        provision['networks'] = cs.ProvisionNetworks()
        provision['volumes'] = cs.ProvisionVolumes(volume_api=self.volume_api,
                                            compute_rpcapi=self.compute_rpcapi,
                                            conductor_api=self.conductor_api)
        provision['instances'] = cs.ProvisionInstances(
                                            compute_rpcapi=self.compute_rpcapi,
                                            conductor_api=self.conductor_api)

        provision.run(context, resource, provision_doc,
                      workflow_request=workflow_request, workflow_type=wf_type)

        instances_ack = {}
        for instance in resource.instances:
            instances_ack[instance['uuid']] = {'ack': 0,
                                               'power_state': None,
                                               'vm_state': None
                                            }

        self.mc.add(context.request_id, instances_ack)

    @lockutils.synchronized(HYPERVISOR_ACK, 'orc-')
    def hypervisor_ack(self, context, **kwargs):

        instance_uuid = kwargs.get("instance_uuid")
        ack = kwargs.get("ack")
        power_state = kwargs.get("power_state")

        instances_ack = self.mc.get(context.request_id)

        instances_ack[instance_uuid]['ack'] = ack
        instances_ack[instance_uuid]['power_state'] = power_state
        request_complete = True

        for key, instance_ack in instances_ack.items():
            if instance_ack['ack'] == 0:
                request_complete = False

        self.mc.set(context.request_id, instances_ack)
        if request_complete:
            reconcile = o_utils.StateChain('reconciliation')
            reconcile['compute_create'] = cs.ReconcileRequest(
                                            conductor_api=self.conductor_api)
            reconcile.run(context, memory_cache=self.mc)
