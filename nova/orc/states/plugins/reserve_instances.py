# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2013 NTT Data.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from oslo.config import cfg

from nova import exception
from nova.compute import power_state
from nova.compute import vm_states
from nova.openstack.common import jsonutils
from nova.openstack.common import log as logging
from nova.orc import states
from nova.orc import utils as orc_utils
from nova import utils

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

        availability_zone, forced_host = self._handle_availability_zone(
                      resource.availability_zone)

        filter_properties = dict(scheduler_hints=resource.scheduler_hints)
        if forced_host:
            filter_properties['force_hosts'] = [forced_host]

        base_options = {
            'reservation_id': resource.reservation_id,
            'image_ref': resource.image_href,
            'kernel_id': resource.kernel_id or '',
            'ramdisk_id': resource.ramdisk_id or '',
            'power_state': power_state.NOSTATE,
            'vm_state': vm_states.BUILDING,
            'config_drive_id': resource.config_drive_id or '',
            'config_drive': resource.config_drive or '',
            'user_id': context.user_id,
            'project_id': context.project_id,
            'instance_type_id': resource.instance_type['id'],
            'memory_mb': resource.instance_type['memory_mb'],
            'vcpus': resource.instance_type['vcpus'],
            'root_gb': resource.instance_type['root_gb'],
            'ephemeral_gb': resource.instance_type['ephemeral_gb'],
            'display_name': resource.display_name,
            'display_description': resource.display_description,
            'user_data': resource.user_data,
            'key_name': resource.key_name,
            'key_data': resource.key_data,
            'locked': False,
            'metadata': resource.metadata,
            'access_ip_v4': resource.access_ip_v4,
            'access_ip_v6': resource.access_ip_v6,
            'availability_zone': resource.availability_zone,
            'root_device_name': resource.root_device_name,
            'progress': 0,
            'system_metadata': resource.system_metadata}

        options_from_image = self._inherit_properties_from_image(
                resource.image, resource.auto_disk_config)

        base_options.update(options_from_image)

        instance_uuids = [instance['uuid'] for instance in resource.instances]

        request_spec = {
            'image': jsonutils.to_primitive(resource.image),
            'instance_properties': base_options,
            'instance_type': resource.instance_type,
            'instance_uuids': instance_uuids,
            'block_device_mapping': resource.block_device_mapping,
            'security_group': resource.security_group,
        }
        for attempt in range(0, CONF.orchestration.reserve_retry_count):
            inst_host_map = self.scheduler_rpcapi.reserve_instance(context,
                            request_spec=request_spec,
                            admin_password=resource.admin_password,
                            injected_files=resource.injected_files,
                            requested_networks=resource.requested_networks,
                            is_first_time=True,
                            filter_properties=filter_properties)
            fetched_amount = len(inst_host_map)
            if fetched_amount >= desired_instances:
                self.populate_instance_host(resource, inst_host_map)
                break
            else:
                raise exception.ReserveInstancesError()

        return orc_utils.DictableObject(resource=resource,
                                    instance_host_map=inst_host_map)

    @staticmethod
    def _handle_availability_zone(availability_zone):
        # NOTE(vish): We have a legacy hack to allow admins to specify hosts
        #             via az using az:host. It might be nice to expose an
        #             api to specify specific hosts to force onto, but for
        #             now it just supports this legacy hack.
        forced_host = None
        if availability_zone and ':' in availability_zone:
            availability_zone, forced_host = availability_zone.split(':')

        if not availability_zone:
            availability_zone = CONF.default_schedule_zone

        return availability_zone, forced_host

    @staticmethod
    def _inherit_properties_from_image(image, auto_disk_config):
        image_properties = image.get('properties', {})

        def prop(prop_, prop_type=None):
            """Return the value of an image property."""
            value = image_properties.get(prop_)

            if value is not None:
                if prop_type == 'bool':
                    value = utils.bool_from_str(value)

            return value

        options_from_image = {'os_type': prop('os_type'),
                              'architecture': prop('architecture'),
                              'vm_mode': prop('vm_mode')}

        # If instance doesn't have auto_disk_config overridden by request, use
        # whatever the image indicates
        if auto_disk_config is None:
            auto_disk_config = prop('auto_disk_config', prop_type='bool')

        options_from_image['auto_disk_config'] = auto_disk_config
        return options_from_image

    def _instance_update(self, context, instance_uuid, **kwargs):
        """Update an instance in the database using kwargs as value."""

        return self.conductor_api.instance_update(context,
                                                  instance_uuid, **kwargs)

    def populate_instance_host(self, resource, inst_host_map):
        # All well, populate the host parameter for the instances
        for instance in resource.instances:
            if instance['uuid'] in inst_host_map:
                instance['host'] = inst_host_map[instance['uuid']]['host']

    def revert(self, context, result, chain, excp, cause):
        #set the status of the instances to ERROR
        for instance in result.resource.instances:
            instance = self._instance_update(context, instance['uuid'],
                                             vm_state=vm_states.ERROR)
            LOG.debug("Set state of instance %s to ERROR", instance['uuid'])
