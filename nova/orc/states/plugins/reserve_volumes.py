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

from eventlet import greenthread
import time

from nova import block_device
from nova.compute import task_states
from nova.compute import vm_states
from nova import exception
from nova.openstack.common import excutils
from nova.openstack.common import log as logging
from nova.orc import states
from nova.orc import utils as orc_utils

LOG = logging.getLogger(__name__)


class ReserveVolumesDriver(states.ResourceUsingState):

    def __init__(self, **kwargs):
        super(ReserveVolumesDriver, self).__init__(**kwargs)

    def apply(self, context, resource, *args, **kwargs):
        instance_volume_map = {}
        for instance in resource.instances:
            # These validations should happen in validation chain
            self._validate_bdm(context, instance)
            self._populate_instance_for_bdm(context, instance,
                                            resource.instance_type,
                                            resource.image,
                                            resource.block_device_mapping)
            bdms = self.conductor_api.block_device_mapping_get_all_by_instance(
                context, instance)

            self.conductor_api.instance_update(context,
                                instance['uuid'],
                                vm_state=vm_states.BUILDING,
                                task_state=task_states.BLOCK_DEVICE_MAPPING)

            block_device_info = self._prep_block_device(context,
                                                        instance, bdms)
            instance_volume_map[instance['uuid']] = block_device_info

        return orc_utils.DictableObject(resource=resource,
                                    instance_volume_map=instance_volume_map)

    def _validate_bdm(self, context, instance):
        for bdm in self.db.block_device_mapping_get_all_by_instance(
            context, instance['uuid']):
            # NOTE(vish): For now, just make sure the volumes are accessible.
            snapshot_id = bdm.get('snapshot_id')
            volume_id = bdm.get('volume_id')
            if volume_id is not None:
                try:
                    self.volume_api.get(context, volume_id)
                except Exception:
                    raise exception.InvalidBDMVolume(id=volume_id)
            elif snapshot_id is not None:
                try:
                    self.volume_api.get_snapshot(context, snapshot_id)
                except Exception:
                    raise exception.InvalidBDMSnapshot(id=snapshot_id)

    def _populate_instance_for_bdm(self, context, instance, instance_type,
                                   image, block_device_mapping):
        """Populate instance block device mapping information."""
        # FIXME(comstud): Why do the block_device_mapping DB calls
        # require elevated context?
        elevated = context.elevated()
        instance_uuid = instance['uuid']
        image_properties = image.get('properties', {})
        mappings = image_properties.get('mappings', [])
        if mappings:
            self._update_image_block_device_mapping(elevated,
                instance_type, instance_uuid, mappings)

        image_bdm = image_properties.get('block_device_mapping', [])
        for mapping in (image_bdm, block_device_mapping):
            if not mapping:
                continue
            self._update_block_device_mapping(elevated,
                instance_type, instance_uuid, mapping)

    def _update_image_block_device_mapping(self, elevated_context,
                                           instance_type, instance_uuid,
                                           mappings):
        """tell vm driver to create ephemeral/swap device at boot time by
        updating BlockDeviceMapping
        """
        for bdm in block_device.mappings_prepend_dev(mappings):
            LOG.debug(_("bdm %s"), bdm, instance_uuid=instance_uuid)

            virtual_name = bdm['virtual']
            if virtual_name == 'ami' or virtual_name == 'root':
                continue

            if not block_device.is_swap_or_ephemeral(virtual_name):
                continue

            size = self._volume_size(instance_type, virtual_name)
            if size == 0:
                continue

            values = {
                'instance_uuid': instance_uuid,
                'device_name': bdm['device'],
                'virtual_name': virtual_name,
                'volume_size': size}
            self.db.block_device_mapping_update_or_create(elevated_context,
                values)

    def _update_block_device_mapping(self, elevated_context,
                                     instance_type, instance_uuid,
                                     block_device_mapping):
        """tell vm driver to attach volume at boot time by updating
        BlockDeviceMapping
        """
        LOG.debug(_("block_device_mapping %s"), block_device_mapping,
            instance_uuid=instance_uuid)
        for bdm in block_device_mapping:
            assert 'device_name' in bdm

            values = {'instance_uuid': instance_uuid}
            for key in ('device_name', 'delete_on_termination', 'virtual_name',
                        'snapshot_id', 'volume_id', 'volume_size',
                        'no_device'):
                values[key] = bdm.get(key)

            virtual_name = bdm.get('virtual_name')
            if (virtual_name is not None and
                block_device.is_swap_or_ephemeral(virtual_name)):
                size = self._volume_size(instance_type, virtual_name)
                if size == 0:
                    continue
                values['volume_size'] = size

            # NOTE(yamahata): NoDevice eliminates devices defined in image
            #                 files by command line option.
            #                 (--block-device-mapping)
            if virtual_name == 'NoDevice':
                values['no_device'] = True
                for k in ('delete_on_termination', 'virtual_name',
                          'snapshot_id', 'volume_id', 'volume_size'):
                    values[k] = None

            self.db.block_device_mapping_update_or_create(elevated_context,
                values)

    @staticmethod
    def _volume_size(instance_type, virtual_name):
        size = 0
        if virtual_name == 'swap':
            size = instance_type.get('swap', 0)
        elif block_device.is_ephemeral(virtual_name):
            num = block_device.ephemeral_num(virtual_name)

            # TODO(yamahata): ephemeralN where N > 0
            # Only ephemeral0 is allowed for now because InstanceTypes
            # table only allows single local disk, ephemeral_gb.
            # In order to enhance it, we need to add a new columns to
            # instance_types table.
            if num > 0:
                return 0

            size = instance_type.get('ephemeral_gb')

        return size

    def _setup_block_device_mapping(self, context, instance, bdms):
        """setup volumes for block device mapping."""
        block_device_mapping = []
        swap = None
        ephemerals = []

        for bdm in bdms:
            LOG.debug(_('Setting up bdm %s'), bdm, instance=instance)

            if bdm['no_device']:
                continue
            if bdm['virtual_name']:
                virtual_name = bdm['virtual_name']
                device_name = bdm['device_name']
                assert block_device.is_swap_or_ephemeral(virtual_name)
                if virtual_name == 'swap':
                    swap = {'device_name': device_name,
                            'swap_size': bdm['volume_size']}
                elif block_device.is_ephemeral(virtual_name):
                    eph = {'num': block_device.ephemeral_num(virtual_name),
                           'virtual_name': virtual_name,
                           'device_name': device_name,
                           'size': bdm['volume_size']}
                    ephemerals.append(eph)
                continue

            if ((bdm['snapshot_id'] is not None) and
                (bdm['volume_id'] is None)):
                # TODO(yamahata): default name and description
                snapshot = self.volume_api.get_snapshot(context,
                    bdm['snapshot_id'])
                vol = self.volume_api.create(context, bdm['volume_size'],
                    '', '', snapshot)
                self._await_block_device_map_created(context, vol['id'])
                self.conductor_api.block_device_mapping_update(
                    context, bdm['id'], {'volume_id': vol['id']})
                bdm['volume_id'] = vol['id']

            block_device_mapping.append(bdm)

        block_device_info = {
            'root_device_name': instance['root_device_name'],
            'swap': swap,
            'ephemerals': ephemerals,
            'block_device_mapping': block_device_mapping
        }

        return block_device_info

    def _await_block_device_map_created(self, context, vol_id, max_tries=30,
                                        wait_between=1):
        # TODO(yamahata): creating volume simultaneously
        # reduces creation time?
        # TODO(yamahata): eliminate dumb polling
        # TODO(harlowja): make the max_tries configurable or dynamic?
        attempts = 0
        start = time.time()
        while attempts < max_tries:
            volume = self.volume_api.get(context, vol_id)
            volume_status = volume['status']
            if volume_status != 'creating':
                if volume_status != 'available':
                    LOG.warn(_("Volume id: %s finished being created but was"
                               " not set as 'available'"), vol_id)
                    # NOTE(harlowja): return how many attempts were tried
                return attempts + 1
            greenthread.sleep(wait_between)
            attempts += 1
            # NOTE(harlowja): Should only happen if we ran out of attempts
        raise exception.VolumeNotCreated(volume_id=vol_id,
            seconds=int(time.time() - start),
            attempts=attempts)

    def _prep_block_device(self, context, instance, bdms):
        """Set up the block device for an instance with error logging."""
        try:
            return self._setup_block_device_mapping(context, instance, bdms)
        except Exception:  # TODO(rohit): Move this to revert()
            with excutils.save_and_reraise_exception():
                LOG.exception(_('Instance failed block device setup'),
                    instance=instance)

    def revert(self, context, result, chain, excp, cause):
        pass
