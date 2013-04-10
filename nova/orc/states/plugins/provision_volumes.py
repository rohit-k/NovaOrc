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

from nova.openstack.common import jsonutils
from nova.openstack.common import log as logging
from nova.orc import states
from nova.orc import orc_utils


CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class ProvisionVolumesDriver(states.ResourceUsingState):
    """Driver that implements volume provisioning"""
    def apply(self, context, resource, provision_doc):
        volumes = provision_doc.volumes
        instance_volume_map = {}
        block_device_mapping = []

        for instance in resource.instances:
            block_device_info = volumes.get(instance['uuid'])
            if not block_device_info:
                continue
            for bdm in block_device_info['block_device_mapping']:
                if bdm['volume_id'] is not None:
                    volume = self.volume_api.get(context, bdm['volume_id'])
                    self.volume_api.check_attach(context, volume,
                                                 instance=instance)

                    cinfo = self.compute_rpcapi.attach_volume_boot(context,
                                                         instance,
                                                         volume,
                                                         bdm['device_name'])
                    self.conductor_api.block_device_mapping_update(
                        context, bdm['id'],
                        {'connection_info': jsonutils.dumps(cinfo)})
                    bdmap = {'connection_info': cinfo,
                        'mount_device': bdm['device_name'],
                        'delete_on_termination': bdm['delete_on_termination']}
                    block_device_mapping.append(bdmap)

                block_device_info['block_device_mapping'] = \
                                                        block_device_mapping
                instance_volume_map[instance['uuid']] = block_device_info
        provision_doc.volumes = instance_volume_map
        return orc_utils.DictableObject()
