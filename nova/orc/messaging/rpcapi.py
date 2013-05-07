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

"""
Client side of the Orchestration RPC API.
"""

from oslo.config import cfg

from nova.openstack.common import log as logging
import nova.openstack.common.rpc.proxy


LOG = logging.getLogger(__name__)

CONF = cfg.CONF
CONF.import_opt('topic', 'nova.orc.opts', group='orchestration')


class OrchestrationAPI(nova.openstack.common.rpc.proxy.RpcProxy):
    '''Client side of the orchestration rpc API.

    API version history:

        1.0 - Initial version.
    '''

    #
    # NOTE(russellb): This is the default minimum version that the server
    # (manager) side must implement unless otherwise specified using a version
    # argument to self.call()/cast()/etc. here.  It should be left as X.0 where
    # X is the current major API version (1.0, 2.0, ...).  For more information
    # about rpc API versioning, see the docs in
    # openstack/common/rpc/dispatcher.py.
    #
    BASE_RPC_API_VERSION = '1.0'

    def __init__(self):
        super(OrchestrationAPI, self).__init__(
              topic=CONF.orchestration.topic,
              default_version=self.BASE_RPC_API_VERSION)

    def create_instance(self, ctxt, instance_type, image_uuid, **kwargs):
        kwargs.update(instance_type=instance_type, image_uuid=image_uuid)
        return self.call(ctxt, self.make_msg('fulfill_compute_create',
                                             **kwargs))

    def reserve_and_provision_resources(self, ctxt, resource):
        self.cast(ctxt, self.make_msg('reserve_and_provision_resources',
                                      resource=resource))
