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


from kazoo import exceptions

from nova.openstack.common import jsonutils
from nova.openstack.common import log as logging
from nova.orc.backends.driver import WorkflowPersistentBackendDriver
from nova.orc.zk.proxy import ZkProxy


LOG = logging.getLogger(__name__)

QUEUES = ['/resources']

HISTORY_PATH = '/resource-histories'
NODE_PREFIX = 'resource-'
BASE_ZK_API_VERSION = '1.0'


class ZookeeperDriver(WorkflowPersistentBackendDriver):
    """Driver that stores data to the zk queue"""

    def __init__(self, **kwargs):
        self.zk = ZkProxy(QUEUES[0], BASE_ZK_API_VERSION)
        self.history_path = None

    def resource_tracker_actions_get(self, context, tracking_id):
        self.history_path = HISTORY_PATH + '/' + tracking_id
        try:
            paths = self.zk.get_children(context, self.history_path + "/")
        except exceptions.NoNodeError:
            paths = []
        for p in paths:
            if not p.startswith(NODE_PREFIX):
                continue
            try:
                (data, _zk) = self.zk.get_data(context,
                                               self.history_path + "/%s" % (p))
                if data:
                    return(jsonutils.loads(data))
            except exceptions.NoNodeError:
                pass

    def resource_tracker_create(self, context, data):
        self.history_path = HISTORY_PATH + '/' + data['tracking_id']
        self.zk.create_node(context, self.history_path + "/%s" % (NODE_PREFIX),
                data)

    def resource_tracker_action_create(self, context, data):
        self.history_path = HISTORY_PATH + '/' + data['tracking_id']
        self.zk.create_node(context, self.history_path + "/%s" % (NODE_PREFIX),
                data)

    # A method to update the zk-node to set status of action to
    # complete(would be called from the orc/states/compute)
    def resource_tracker_update(self, context, tracking_id, data):
        self.history_path = HISTORY_PATH + '/' + tracking_id
        self.zk.set_data(context, self.history_path + "/%s" % (NODE_PREFIX),
                         data)
