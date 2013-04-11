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

from oslo.config import cfg

from nova.compute import task_states
from nova.compute import vm_states
from nova.openstack.common import log as logging
from nova.orc import states
from nova.orc import utils as orc_utils


CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class ProvisionInstancesDriver(states.ResourceUsingState):
    """Driver that implements instance provisioning"""

    def __init__(self, **kwargs):
        super(ProvisionInstancesDriver, self).__init__(**kwargs)

    def apply(self, context, resource, provision_doc, **kwargs):
        instances = provision_doc.instances.instance_host_map
        networks = provision_doc.networks.instance_network_map
        LOG.debug(provision_doc.volumes)
        volumes = provision_doc.volumes.instance_volume_map

        for instance in resource.instances:
            weighed_host = instances[instance['uuid']]
            network_info = networks[instance['uuid']]
            block_device_info = volumes.get(instance['uuid'])

            instance = self.conductor_api.instance_update(context,
                                              instance['uuid'],
                                              vm_state=vm_states.BUILDING,
                                              task_state=task_states.SPAWNING)

            self.compute_rpcapi.orc_run_instance(context, instance=instance,
                                host=weighed_host['host'],
                                request_spec=resource.request_spec,
                                filter_properties=resource.filter_properties,
                                network_info=network_info,
                                block_device_info=block_device_info,
                                injected_files=resource.injected_files,
                                admin_password=resource.admin_password,
                                is_first_time=True,
                                node=None)
                            # TODO(rohit): Hard-set node=None above for now
                            # since as scheduler does weighed_host.obj.nodename

        return orc_utils.DictableObject()

    def revert(self, context, result, chain, excp, cause):
        pass
