# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2013, NTT Data Inc.
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

"""
Client side of the Zookeeper messaging API.
"""

from oslo.config import cfg

from nova.openstack.common import log as logging
import nova.orc.zk.proxy


LOG = logging.getLogger(__name__)

CONF = cfg.CONF
CONF.import_opt('zookeeper_queue_path', 'nova.orc.opts', group='orchestration')


class OrchestrationAPI(nova.orc.zk.proxy.ZkProxy):
    '''Client side of the orchestration zookeeper API.

    API version history:

        1.0 - Initial version.
    '''

    BASE_ZK_API_VERSION = '1.0'

    def __init__(self):
        super(OrchestrationAPI, self).__init__(
            queue=CONF.orchestration.zookeeper_queue_path,
            default_version=self.BASE_ZK_API_VERSION)

    def reserve_and_provision_resources(self, ctxt, resource):
        self.put_message_in_queue(ctxt, self.make_msg(
                                            'reserve_and_provision_resources',
                                            resource=resource))
