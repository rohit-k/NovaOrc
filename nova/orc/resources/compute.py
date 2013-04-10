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

from nova.openstack.common import log as logging

LOG = logging.getLogger(__name__)


def _multi_get_first(where, *keys):
    # Used to get a single option from the first key
    # it is found in the provided dictionary
    for k in keys:
        if k in where:
            return where[k]
    return None


class Display(dict):
    def __init__(self, name, description):
        super(Display, self).__init__()
        self.name = name
        self.description = description


class Create(object):
    def __init__(self, **kwargs):
        self.instance_type = kwargs.get('instance_type')
        self.image_href = kwargs.get('image_uuid')
        self.kernel_id = kwargs.get('kernel_id')
        self.ramdisk_id = kwargs.get('ramdisk_id')

        self.min_count = kwargs.get('min_count')
        self.max_count = kwargs.get('max_count')

        display_name = kwargs.get('display_name')
        display_description = _multi_get_first(kwargs, 'display_description',
                                               'display_name')
        self.display = Display(display_name, display_description)

        self.key_name = kwargs.get('key_name')
        self.key_data = None
        self.key_pair = _multi_get_first(kwargs, 'key_name', 'key_pair')
        self.security_group = kwargs.get('security_group')
        self.availability_zone = kwargs.get('availability_zone')
        self.user_data = kwargs.get('user_data')
        self.metadata = kwargs.get('metadata')
        self.injected_files = kwargs.get('injected_files')
        self.admin_password = kwargs.get('admin_password')
        self.access_ip_v4 = kwargs.get('access_ip_v4')
        self.access_ip_v6 = kwargs.get('access_ip_v6')
        self.requested_networks = kwargs.get('requested_networks')
        self.config_drive = kwargs.get('config_drive')
        self.block_device_mapping = kwargs.get('block_device_mapping')
        self.auto_disk_config = kwargs.get('auto_disk_config')
        self.scheduler_hints = kwargs.get('scheduler_hints')

        # additional attributes needed by a resource
        self.reservation_id = kwargs.get('reservation_id', None)
        self.image = kwargs.get('image', None)
        self.num_instances = kwargs.get('num_instances', None)
        self.quota_reservations = kwargs.get('quota_reservations', None)
        self.config_drive_id = kwargs.get('config_drive_id', None)
        self.root_device_name = kwargs.get('root_device_name', None)
        self.forced_host = kwargs.get('forced_host', None)
        self.instances = kwargs.get('instances', [])
        self.workflow_request_id = kwargs.get('workflow_request_id')
        self.request_spec = kwargs.get('request_spec', None)
        self.filter_properties = kwargs.get('filter_properties', None)

    def to_dict(self):
        return {
            "instance_type": self.instance_type,
            "image_href": self.image_href,
            "kernel_id": self.kernel_id,
            "ramdisk_id": self.ramdisk_id,
            "min_count": self.min_count,
            "max_count": self.max_count,
            "display": self.display,
            "key_name": self.key_name,
            "key_data": self.key_data,
            "key_pair": self.key_pair,
            "security_group": self.security_group,
            "availability_zone": self.availability_zone,
            "user_data": self.user_data,
            "metadata": self.metadata,
            "injected_files": self.injected_files,
            "admin_password": self.admin_password,
            "access_ip_v4": self.access_ip_v4,
            "access_ip_v6": self.access_ip_v6,
            "requested_networks": self.requested_networks,
            "config_drive": self.config_drive,
            "block_device_mapping": self.block_device_mapping,
            "auto_disk_config": self.auto_disk_config,
            "scheduler_hints": self.scheduler_hints,

            # additional attributes needed by a resource
            "reservation_id": self.reservation_id,
            "image": self.image,
            "num_instances": self.num_instances,
            "quota_reservations": self.quota_reservations,
            "config_drive_id": self.config_drive_id,
            "root_device_name": self.root_device_name,
            "forced_host": self.forced_host,
            "instances": self.instances,
            "workflow_request_id": self.workflow_request_id,
            "request_spec": self.request_spec,
            "filter_properties": self.filter_properties
        }


class Instance(object):
    def __init__(self, create_resource):
        self.create_resource = create_resource
        self.target = None


class MultiProvisionDocument(object):
    def __init__(self):
        self.instances = []
        self.networks = []
        self.volumes = []
        self.images = []

    def to_dict(self):
        return {
            "instances": self.instances,
            "networks": self.networks,
            "volumes": self.volumes,
            "images": self.images
        }
