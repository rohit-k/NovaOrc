#    Copyright 2012 IBM Corp.
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

"""Handles all requests to the orchestration service."""

from nova.compute import api as compute_api
from nova.orc.messaging import rpcapi as orc_rpcapi
from nova.openstack.common import log as logging

LOG = logging.getLogger(__name__)


class ComputeOrchestrationAPI(compute_api.API):
    """Orchestration API that does updates via RPC to OrchestrationManager."""

    def __init__(self, *args, **kwargs):
        super(ComputeOrchestrationAPI, self).__init__(*args, **kwargs)
        self.orc_rpcapi = orc_rpcapi.OrchestrationAPI()

    def create(self, context, instance_type, image_uuid, **kwargs):
        """Provision instances, using an orchestration layer that manages
        resource reservations and communicates directly with compute

        Returns a tuple of (instances, reservation_id)
        """
        return self.orc_rpcapi.create_instance(context, instance_type,
                                               image_uuid, **kwargs)

    def delete(self, context, instance):
        """Terminate an instance."""
        super(ComputeOrchestrationAPI, self).delete(context, instance)


class HostAPI(compute_api.HostAPI):
    """HostAPI() class for orchestration service"""

    def __init__(self):
        super(HostAPI, self).__init__()


class InstanceActionAPI(compute_api.InstanceActionAPI):
    """InstanceActionAPI() class for orchestration service"""

    def __init__(self):
        super(InstanceActionAPI, self).__init__()
