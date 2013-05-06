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


from nova.db import base
from nova.openstack.common import log as logging
from nova.orc.backends.driver import WorkflowPersistentBackendDriver


LOG = logging.getLogger(__name__)


class SqlDriver(WorkflowPersistentBackendDriver):
    """Driver that stores data to the db"""
    def __init__(self, **kwargs):
        db_base = base.Base()
        self.db = db_base.db

    def resource_tracker_actions_get(self, context, tracking_id):
        return self.db.resource_tracker_actions_get(context, tracking_id)

    def resource_tracker_create(self, context, data):
        self.db.resource_tracker_create(context, data)

    def resource_tracker_action_create(self, context, data):
        self.db.resource_tracker_action_create(context, data)
