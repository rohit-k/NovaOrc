# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
#    Copyright 2013 NTT Data.
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

import base64
import datetime
import time
import uuid

from oslo.config import cfg

from nova import block_device
from nova.compute import instance_actions
from nova.compute import instance_types
from nova.compute import power_state
from nova.compute import task_states
from nova.compute import utils as compute_utils
from nova.compute import vm_states
from nova import exception
from nova.image import glance
from nova import network
from nova.network.security_group import openstack_driver
from nova import notifications
from nova.openstack.common import excutils
from nova.openstack.common import jsonutils
from nova.openstack.common import timeutils
from nova.openstack.common import log as logging
from nova.orc import states
from nova.orc import utils as orc_utils
import nova.policy
from nova import quota
from nova import utils


LOG = logging.getLogger(__name__)
validate_boot_opts = [
    cfg.IntOpt('validate_boot_timeout',
        default=900,
        help='Time in seconds to check if instance is in ACTIVE state.'),
    cfg.IntOpt('validate_boot_check_interval',
        default=10,
        help='Time interval in seconds to sleep until timeout'),
]

CONF = cfg.CONF
CONF.register_opts(validate_boot_opts, group='orchestration')
CONF.import_opt('allow_resize_to_same_host', 'nova.compute.api')
CONF.import_opt('default_schedule_zone', 'nova.compute.api')
CONF.import_opt('multi_instance_display_name_template', 'nova.compute.api')
CONF.import_opt('null_kernel', 'nova.compute.api')
MAX_USERDATA_SIZE = 65535
QUOTAS = quota.QUOTAS


def _check_policy(context, action, target, scope='compute'):
    _action = '%s:%s' % (scope, action)
    nova.policy.enforce(context, _action, target)


# This chain is responsible for validating that a desired resource
# validates against a given policy for that resource under the
# given context so that before we start acting on the desired resources
# we have at least ensured that the resource 'ask' passes some level of
# sanity.
class ValidateResourcePolicies(states.ResourceUsingState):

    def apply(self, context, resource):
        self._check_create_policies(context, resource.availability_zone,
                                    resource.requested_networks,
                                    resource.block_device_mapping)
        return orc_utils.DictableObject(details='policies_validated',
                                        resource=resource)

    def _check_create_policies(self, context, availability_zone,
                               requested_networks, block_device_mapping):
        """Check policies for create()."""
        target = {
            'project_id': context.project_id,
            'user_id': context.user_id,
            'availability_zone': availability_zone
        }
        _check_policy(context, 'create', target)

        if requested_networks:
            _check_policy(context, 'create:attach_network', target)

        if block_device_mapping:
            _check_policy(context, 'create:attach_volume', target)


# This validation state checks the image resource is correct and is
# accessible by the context asking for said image resource.
class ValidateResourceImage(states.ResourceUsingState):

    @staticmethod
    def _handle_kernel_and_ramdisk(context, kernel_id, ramdisk_id,
                                   image):
        """Choose kernel and ramdisk appropriate for the instance.

        The kernel and ramdisk can be chosen in one of three ways:
            1. Passed in with create-instance request.
            2. Inherited from image.
            3. Forced to None by using `null_kernel` FLAG.
        """
        # Inherit from image if not specified
        image_properties = image.get('properties', {})

        if kernel_id is None:
            kernel_id = image_properties.get('kernel_id')

        if ramdisk_id is None:
            ramdisk_id = image_properties.get('ramdisk_id')

        # Force to None if using null_kernel
        if kernel_id == str(CONF.null_kernel):
            kernel_id = None
            ramdisk_id = None

        # Verify kernel and ramdisk exist (fail-fast)
        if kernel_id is not None:
            image_service, kernel_id = glance.get_remote_image_service(
                context, kernel_id)
            image_service.show(context, kernel_id)

        if ramdisk_id is not None:
            image_service, ramdisk_id = glance.get_remote_image_service(
                context, ramdisk_id)
            image_service.show(context, ramdisk_id)

        return kernel_id, ramdisk_id

    def apply(self, context, resource, **kwargs):

        if resource.image_href:
            (image_service, image_id) = glance.get_remote_image_service(
                                                 context, resource.image_href)
            resource.image = jsonutils.to_primitive(image_service.show(
                                                                    context,
                                                                    image_id))
            if resource.image['status'] != 'active':
                raise exception.ImageNotActive(image_id=image_id)
        else:
            resource.image = {}

        if resource.instance_type['memory_mb'] < int(
                                           resource.image.get('min_ram') or 0):
            raise exception.InstanceTypeMemoryTooSmall()
        if resource.instance_type['root_gb'] < int(
                                          resource.image.get('min_disk') or 0):
            raise exception.InstanceTypeDiskTooSmall()

        resource.kernel_id, resource.ramdisk_id = \
                                       self._handle_kernel_and_ramdisk(context,
                                                           resource.kernel_id,
                                                           resource.ramdisk_id,
                                                           resource.image)

        return orc_utils.DictableObject(details='image_validated',
                                        resource=resource)


class ValidateResourceRequest(states.ResourceUsingState):
    """Validate request parameters for quota reservations etc"""

    def _check_num_instances_quota(self, context, instance_type, min_count,
                                   max_count):
        """Enforce quota limits on number of instances created."""

        # Determine requested cores and ram
        req_cores = max_count * instance_type['vcpus']
        req_ram = max_count * instance_type['memory_mb']

        # Check the quota
        try:
            reservations = QUOTAS.reserve(context, instances=max_count,
                                          cores=req_cores, ram=req_ram)
        except exception.OverQuota as exc:
            # OK, we exceeded quota; let's figure out why...
            quotas = exc.kwargs['quotas']
            usages = exc.kwargs['usages']
            overs = exc.kwargs['overs']

            headroom = dict((res, quotas[res] -
                             (usages[res]['in_use'] + usages[res]['reserved']))
                for res in quotas.keys())

            allowed = headroom['instances']

            # Reduce 'allowed' instances in line with the cores & ram headroom
            if instance_type['vcpus']:
                allowed = min(allowed,
                    headroom['cores'] // instance_type['vcpus'])
            if instance_type['memory_mb']:
                allowed = min(allowed,
                    headroom['ram'] // instance_type['memory_mb'])

            # Convert to the appropriate exception message
            if allowed <= 0:
                msg = _("Cannot run any more instances of this type.")
                allowed = 0
            elif min_count <= allowed <= max_count:
                # We're actually OK, but still need reservations
                return self._check_num_instances_quota(context, instance_type,
                    min_count, allowed)
            else:
                msg = (_("Can only run %s more instances of this type.") %
                       allowed)

            overlimit_resource = overs[0]
            used = quotas[overlimit_resource] - headroom[overlimit_resource]
            total_allowed = used + headroom[overlimit_resource]
            overs = ','.join(overs)

            pid = context.project_id
            LOG.warn(_("%(overs)s quota exceeded for %(pid)s,"
                       " tried to run %(min_count)s instances. %(msg)s"),
                locals())
            requested = dict(instances=min_count, cores=req_cores, ram=req_ram)
            raise exception.TooManyInstances(overs=overs,
                req=requested[overlimit_resource],
                used=used, allowed=total_allowed,
                resource=overlimit_resource)

        return max_count, reservations

    def _check_metadata_properties_quota(self, context, metadata=None):
        """Enforce quota limits on metadata properties."""
        if not metadata:
            metadata = {}
        num_metadata = len(metadata)
        try:
            QUOTAS.limit_check(context, metadata_items=num_metadata)
        except exception.OverQuota as exc:
            pid = context.project_id
            LOG.warn(_("Quota exceeded for %(pid)s, tried to set "
                       "%(num_metadata)s metadata properties") % locals())
            quota_metadata = exc.kwargs['quotas']['metadata_items']
            raise exception.MetadataLimitExceeded(allowed=quota_metadata)

        # Because metadata is stored in the DB, we hard-code the size limits
        # In future, we may support more variable length strings, so we act
        #  as if this is quota-controlled for forwards compatibility
        for k, v in metadata.iteritems():
            if len(k) == 0:
                msg = _("Metadata property key blank")
                LOG.warn(msg)
                raise exception.InvalidMetadata(reason=msg)
            if len(k) > 255:
                msg = _("Metadata property key greater than 255 characters")
                LOG.warn(msg)
                raise exception.InvalidMetadataSize(reason=msg)
            if len(v) > 255:
                msg = _("Metadata property value greater than 255 characters")
                LOG.warn(msg)
                raise exception.InvalidMetadataSize(reason=msg)

    def _check_injected_file_quota(self, context, injected_files):
        """Enforce quota limits on injected files.

        Raises a QuotaError if any limit is exceeded.
        """
        if injected_files is None:
            return

        # Check number of files first
        try:
            QUOTAS.limit_check(context, injected_files=len(injected_files))
        except exception.OverQuota:
            raise exception.OnsetFileLimitExceeded()

        # OK, now count path and content lengths; we're looking for
        # the max...
        max_path = 0
        max_content = 0
        for path, content in injected_files:
            max_path = max(max_path, len(path))
            max_content = max(max_content, len(content))

        try:
            QUOTAS.limit_check(context, injected_file_path_bytes=max_path,
                               injected_file_content_bytes=max_content)
        except exception.OverQuota as exc:
            # Favor path limit over content limit for reporting
            # purposes
            if 'injected_file_path_bytes' in exc.kwargs['overs']:
                raise exception.OnsetFilePathLimitExceeded()
            else:
                raise exception.OnsetFileContentLimitExceeded()

    def _check_requested_networks(self, context, requested_networks):
        """
        Check if the networks requested belongs to the project
        and the fixed IP address for each network provided is within
        same the network block
        """
        if not requested_networks:
            return

        network.api.validate_networks(context, requested_networks)

    def _handle_availability_zone(self, availability_zone):
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

    def apply(self, context, resource):

        if not resource.metadata:
            resource.metadata = {}
        if not resource.security_group:
            resource.security_group = 'default'

        if not resource.instance_type:
            resource.instance_type = instance_types.get_default_instance_type()
        if not resource.min_count:
            resource.min_count = 1
        if not resource.max_count:
            resource.max_count = resource.min_count

        resource.block_device_mapping = resource.block_device_mapping or []
        if resource.instance_type['disabled']:
            raise exception.InstanceTypeNotFound(
                    instance_type_id=resource.instance_type['id'])

        if resource.user_data:
            l = len(resource.user_data)
            if l > MAX_USERDATA_SIZE:
                # NOTE(mikal): user_data is stored in a text column, and
                # the database might silently truncate if its over length.
                raise exception.InstanceUserDataTooLarge(
                    length=l, maxsize=MAX_USERDATA_SIZE)

            try:
                base64.decodestring(resource.user_data)
            except base64.binascii.Error:
                raise exception.InstanceUserDataMalformed()

        # Reserve quotas
        resource.num_instances, resource.quota_reservations = \
                        self._check_num_instances_quota(context,
                                                        resource.instance_type,
                                                        resource.min_count,
                                                        resource.max_count)

        self._check_metadata_properties_quota(context, resource.metadata)
        self._check_injected_file_quota(context, resource.injected_files)
        self._check_requested_networks(context, resource.requested_networks)

        # Handle config_drive
        resource.config_drive_id = None
        if resource.config_drive and not utils.is_valid_boolstr(
                                                        resource.config_drive):
            # config_drive is volume id
            resource.config_drive_id = resource.config_drive
            resource.config_drive = None

            # Ensure config_drive image exists
            cd_image_service, config_drive_id = \
                      glance.get_remote_image_service(context,
                                                      resource.config_drive_id)
            cd_image_service.show(context, resource.config_drive_id)

        if resource.key_data is None and resource.key_name:
            resource.key_pair = self.db.key_pair_get(context, context.user_id,
                                            resource.key_name)
            resource.key_data = resource.key_pair['public_key']

        resource.root_device_name = block_device.properties_root_device_name(
                                          resource.image.get('properties', {}))

        resource.availability_zone, resource.forced_host = \
                     self._handle_availability_zone(resource.availability_zone)

        resource.system_metadata = instance_types.save_instance_type_info(
                                dict(), resource.instance_type)

        return orc_utils.DictableObject(details='request_validated',
                                        resource=resource)

    def revert(self, context, result, chain, excp, cause):
        # Ensure that if we made a reservation that we now undo it.
        if result.resource.quota_reservations:
            QUOTAS.rollback(context, result.resource.quota_reservations)


class CreateComputeEntry(states.ResourceUsingState):

    def _apply_instance_name_template(self, context, instance, index):
        params = {
            'uuid': instance['uuid'],
            'name': instance['display_name'],
            'count': index + 1,
        }
        try:
            new_name = (CONF.multi_instance_display_name_template %
                        params)
        except (KeyError, TypeError):
            LOG.exception(_('Failed to set instance name using '
                            'multi_instance_display_name_template.'))
            new_name = instance['display_name']
        updates = {'display_name': new_name}
        if not instance.get('hostname'):
            updates['hostname'] = utils.sanitize_hostname(new_name)
        instance = self.db.instance_update(context,
                instance['uuid'], updates)
        return instance

    def _populate_instance_shutdown_terminate(self, instance, image,
                                              block_device_mapping):
        """Populate instance shutdown_terminate information."""
        image_properties = image.get('properties', {})
        if (block_device_mapping or
            image_properties.get('mappings') or
            image_properties.get('block_device_mapping')):
            instance['shutdown_terminate'] = False

    def _populate_instance_names(self, instance, num_instances):
        """Populate instance display_name and hostname."""
        display_name = instance.get('display_name')
        hostname = instance.get('hostname')

        if display_name is None:
            display_name = self._default_display_name(instance['uuid'])
            instance['display_name'] = display_name

        if hostname is None and num_instances == 1:
            # NOTE(russellb) In the multi-instance case, we're going to
            # overwrite the display_name using the
            # multi_instance_display_name_template.  We need the default
            # display_name set so that it can be used in the template, though.
            # Only set the hostname here if we're only creating one instance.
            # Otherwise, it will be built after the template based
            # display_name.
            hostname = display_name
            instance['hostname'] = utils.sanitize_hostname(hostname)

    def _default_display_name(self, instance_uuid):
        return "Server %s" % instance_uuid

    def _populate_instance_for_create(self, base_options, image,
                                      security_groups):
        """Build the beginning of a new instance."""
        image_properties = image.get('properties', {})

        instance = base_options
        if not instance.get('uuid'):
            # Generate the instance_uuid here so we can use it
            # for additional setup before creating the DB entry.
            instance['uuid'] = str(uuid.uuid4())

        instance['launch_index'] = 0
        instance['vm_state'] = vm_states.BUILDING
        instance['task_state'] = task_states.SCHEDULING
        instance['info_cache'] = {'network_info': '[]'}

        # Store image properties so we can use them later
        # (for notifications, etc).  Only store what we can.
        instance.setdefault('system_metadata', {})
        for key, value in image_properties.iteritems():
            new_value = str(value)[:255]
            instance['system_metadata']['image_%s' % key] = new_value

        # Keep a record of the original base image that this
        # image's instance is derived from:
        base_image_ref = image_properties.get('base_image_ref')
        if not base_image_ref:
            # base image ref property not previously set through a snapshot.
            # default to using the image ref as the base:
            base_image_ref = base_options['image_ref']

        instance['system_metadata']['image_base_image_ref'] = base_image_ref

        # Use 'default' security_group if none specified.
        if security_groups is None:
            security_groups = ['default']
        elif not isinstance(security_groups, list):
            security_groups = [security_groups]
        instance['security_groups'] = security_groups

        return instance

    #NOTE(bcwaldon): No policy check since this is only used by scheduler and
    # the compute api. That should probably be cleaned up, though.
    def _create_db_entry_for_new_instance(self, context, image,
            base_options, security_group, block_device_mapping, num_instances,
            index):
        """Create an entry in the DB for this new instance,
        including any related table updates (such as security group,
        etc).

        This is called by the scheduler after a location for the
        instance has been determined.
        """
        instance = self._populate_instance_for_create(base_options,
                image, security_group)

        self._populate_instance_names(instance, num_instances)
        self._populate_instance_shutdown_terminate(instance, image,
                                                   block_device_mapping)

        # ensure_default security group is called before the instance
        # is created so the creation of the default security group is
        # proxied to the sgh.
        self.security_group_api.ensure_default(context)
        instance = self.db.instance_create(context, instance)

        if num_instances > 1:
            # NOTE(russellb) We wait until this spot to handle
            # multi_instance_display_name_template, because we need
            # the UUID from the instance.
            instance = self._apply_instance_name_template(context, instance,
                                                          index)
        return instance

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

    def _record_action_start(self, context, instance, action):
        act = compute_utils.pack_action_start(context, instance['uuid'],
                                              action)
        self.db.action_start(context, act)

    def apply(self, context, resource):

        self.security_group_api = \
                    openstack_driver.get_openstack_security_group_driver()

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
            'launch_time': time.strftime('%Y-%m-%dT%H:%M:%SZ',
                time.gmtime()),
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

        LOG.debug(_("Going to run %s instances..."), resource.num_instances)

        filter_properties = dict(scheduler_hints=resource.scheduler_hints)
        if resource.forced_host:
            _check_policy(context, 'create:forced_host', {})
            filter_properties['force_hosts'] = [resource.forced_host]

        resource.filter_properties = filter_properties

        # Create DB Entry for the instances and initiate a workflow request
        for i in xrange(resource.num_instances):
            options = base_options.copy()
            instance = self._create_db_entry_for_new_instance(context,
                                             resource.image,
                                             options,
                                             resource.security_group,
                                             resource.block_device_mapping,
                                             resource.num_instances, i)
            resource.instances.append(jsonutils.to_primitive(instance))

            # send a state update notification for the initial create to
            # show it going from non-existent to BUILDING
            notifications.send_update_with_states(context, instance, None,
                vm_states.BUILDING, None, None, service="api")

        # Commit the reservations
        QUOTAS.commit(context, resource.quota_reservations)

        # Record the starting of instances in the db
        for instance in resource.instances:
            self._record_action_start(context, instance,
                instance_actions.CREATE)

        return orc_utils.DictableObject(details='created_db_entry',
                                        resource=resource)

    def revert(self, context, result, chain, excp, cause):
        # In the case of any exceptions, attempt DB cleanup and rollback the
        # quota reservations.
        with excutils.save_and_reraise_exception():
            try:
                for instance in result.resource.instances:
                    self.db.instance_destroy(context, instance['uuid'])
            finally:
                if result.resource.quota_reservations:
                    QUOTAS.rollback(context,
                                    result.resource.quota_reservations)


class ValidateBooted(states.ResourceUsingState):

    def apply(self, context, resource, provision_doc, backend_driver):

        # TODO: Wait a given amount of time, periodically checking the database
        # to see if the instance has came online, if after X amount of time
        # it has not came online then ack the hypervisor directly to check
        # if its online, if that doesn't work, bail out by performing different
        # types of reconciliation in the revert method here...

        timeout = CONF.orchestration.validate_boot_timeout
        check_interval = CONF.orchestration.validate_boot_check_interval
        provisioning_result = provision_doc.instances.instance_host_map
        instance_uuids = provisioning_result.keys()
        filters = {'uuid': [uuid for uuid in instance_uuids],
                   'vm_state': vm_states.ACTIVE}
        start = datetime.datetime.now()
        all_instances_active = False
        while timeutils.delta_seconds(start, datetime.datetime.now())\
                                                                <= timeout:
            active_instances = self.conductor_api.instance_get_all_by_filters(
                                                              context, filters)
            if len(active_instances) == len(provisioning_result):
                all_instances_active = True
                LOG.debug("All instances ACTIVE. Request tracking %s complete",
                           resource.tracking_id)
                backend_driver.resource_tracker_update(context,
                                                  resource.tracking_id,
                                                  {'status': states.COMPLETED})
                break

            time.sleep(check_interval)
        if not all_instances_active:
            # Check hypervisor ack
            # revert
            pass
        return orc_utils.DictableObject()

    def revert(self, context, result, chain, excp, cause):
        # TODO: perform different types of reconciliation here, possibly a
        # dynamic driver can be loaded that will be given an instance and where
        # it was supposed to be and ask said driver to resolve what to do, the
        # driver could return codes like PASS, REVERT_ALL, etc, where PASS
        # would mean to just leave it alone (likely in error state) and
        # REVERT_ALL would mean to start reverting all changes made
        # (including all instances which did boot correctly...)
        pass
