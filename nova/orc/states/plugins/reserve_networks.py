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
from nova.orc.states import plugins

LOG = logging.getLogger(__name__)


class ReserveNetworksDriver(plugins.ReservationDriver):

    def __init__(self, **kwargs):
        super(ReserveNetworksDriver, self).__init__(**kwargs)
        self.is_quantum_security_groups = (
            openstack_driver.is_quantum_security_groups())

    def _allocate_network(self, context, instance, requested_networks, macs,
                          security_groups):
        """Allocate networks for an instance and return the network info."""
        # NOTE(rohit): Changed the expected_task_state from None to scheduling
        instance = super(ReserveNetworksDriver, self)._instance_update(context,
                                    instance['uuid'],
                                    vm_state=vm_states.BUILDING,
                                    task_state=task_states.NETWORKING,
                                    expected_task_state=task_states.SCHEDULING)
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

    def reserve(self, context, resource):

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

        return instance_network_map

    def unreserve(self, context, resource):

        for instance in resource.instances:
            self.network_api.deallocate_for_instance(context, instance)

    def get(self, context, *args, **kwargs):
        #should return same resource as that in reserve method
        pass
