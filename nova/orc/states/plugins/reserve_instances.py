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

from nova import exception
from nova.compute import vm_states
from nova.openstack.common import log as logging
from nova.orc import states
from nova.orc import utils as orc_utils

retry_opts = [
    cfg.IntOpt('reserve_retry_count',
        default=3,
        help='The number of times to attempt reservation of '
             'instances such that the desired instance count '
             'is achieved.'),
    ]

CONF = cfg.CONF
CONF.register_opts(retry_opts, group='orchestration')
LOG = logging.getLogger(__name__)


class ReserveInstancesDriver(states.ResourceUsingState):
    """Driver that implements instance reservation with a number of retries"""
    def apply(self, context, resource, *args, **kwargs):
        desired_instances = resource.max_count
        inst_host_map = {}
        for attempt in range(0, CONF.orchestration.reserve_retry_count):
            inst_host_map = self.scheduler_rpcapi.reserve_instance(context,
                            request_spec=resource.request_spec,
                            admin_password=resource.admin_password,
                            injected_files=resource.injected_files,
                            requested_networks=resource.requested_networks,
                            is_first_time=True,
                            filter_properties=resource.filter_properties)
            fetched_amount = len(inst_host_map)
            if fetched_amount >= desired_instances:
                self.populate_instance_host(resource, inst_host_map)
                break
            else:
                raise exception.ReserveInstancesError()

    return orc_utils.DictableObject(resource=resource,
                                    instance_host_map=inst_host_map)

    def _instance_update(self, context, instance_uuid, **kwargs):
        """Update an instance in the database using kwargs as value."""

        return self.conductor_api.instance_update(context,
                                                  instance_uuid, **kwargs)

    def populate_instance_host(self, resource, inst_host_map):
        # All well, populate the host parameter for the instances
        for instance in resource.instances:
            if instance['uuid'] in inst_host_map:
                instance['host'] = inst_host_map[instance['uuid']]['host']

    def revert(self, context, result, *args, **kwargs):
        #set the status of the instances to ERROR
        for instance in result.resource.instances:
            instance = self._instance_update(context, instance['uuid'],
                                             vm_state=vm_states.ERROR)
            LOG.debug("Set state of instance %s to ERROR", instance['uuid'])
