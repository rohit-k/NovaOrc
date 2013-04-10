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

from nova.orc import utils as orc_utils

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


class Create(orc_utils.DictableObject):
    def __init__(self, **kwargs):
        # Attributes which we expect to always be there from the initia;
        # create request...
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
