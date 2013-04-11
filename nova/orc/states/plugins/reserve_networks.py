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

from nova.cloudpipe import pipelib
from nova.compute import vm_states
from nova.compute import task_states
from nova.network.security_group import openstack_driver
from nova.openstack.common import excutils
from nova.openstack.common import log as logging
from nova.orc import states
from nova.orc import utils as orc_utils

LOG = logging.getLogger(__name__)


class ReserveNetworksDriver(states.ResourceUsingState):

    def __init__(self, **kwargs):
        super(ReserveNetworksDriver, self).__init__(**kwargs)
        self.is_quantum_security_groups = (
            openstack_driver.is_quantum_security_groups())

    def _allocate_network(self, context, instance, requested_networks, macs,
                          security_groups):
        """Allocate networks for an instance and return the network info."""
        # NOTE(rohit): Changed the expected_task_state from None to scheduling

        self.conductor_api.instance_update(context,
                                    instance['uuid'],
                                    vm_state=vm_states.BUILDING,
                                    task_state=task_states.NETWORKING)

        is_vpn = pipelib.is_vpn_image(instance['image_ref'])
        try:
            # allocate and get network info
            network_info = self.network_api.allocate_for_instance(context,
                                        instance, vpn=is_vpn,
                                        requested_networks=requested_networks,
                                        macs=macs,
                                        conductor_api=self.conductor_api,
                                        security_groups=security_groups)
            return network_info
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.exception(_('Instance failed network setup'),
                    instance=instance)

    def apply(self, context, resource, *args, **kwargs):

        instance_network_map = {}
        if resource.request_spec and self.is_quantum_security_groups:
            security_groups = resource.request_spec.get('security_group')
        else:
            security_groups = []
        for instance in resource.instances:
            macs = self.compute_rpcapi.macs_for_instance(context, instance)
            network_info = self._allocate_network(context, instance,
                resource.requested_networks,
                macs, security_groups)

            instance_network_map[instance['uuid']] = network_info

        return orc_utils.DictableObject(resource=resource,
                                    instance_network_map=instance_network_map)

    def revert(self, context, result, chain, excp, cause):
        for instance in result.resource.instances:
            self.network_api.deallocate_for_instance(context, instance)
