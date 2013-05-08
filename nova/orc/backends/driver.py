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


from nova.openstack.common import log as logging

LOG = logging.getLogger(__name__)


class WorkflowPersistentBackendDriver(object):

    def __init__(self):
        super(WorkflowPersistentBackendDriver, self).__init__()

    def __str__(self):
        return self.__class__.__name__

    def resource_tracker_actions_get(self, context, data, *args, **kwargs):
        """Must be overriden to get action performed by tracking id."""
        msg = _("Driver must implement this method to get actions performed")
        raise NotImplementedError(msg)

    def resource_tracker_create(self, context, data, *args, **kwargs):
        """Must be overriden to store data for storage mechanism to work."""
        msg = _("Driver must implement store mechanism")
        raise NotImplementedError(msg)

    def resource_tracker_action_create(self, context, data, *args, **kwargs):
        """Must be overriden to store data for storage mechanism to work."""
        msg = _("Driver must implement store mechanism")
        raise NotImplementedError(msg)
