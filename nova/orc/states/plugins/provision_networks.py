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

from nova.openstack.common import log as logging
from nova.orc.states import plugins


CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class ProvisionNetworksDriver(plugins.ProvisioningDriver):
    """Driver that implements network provisioning"""

    def __init__(self, **kwargs):
        super(ProvisionNetworksDriver, self).__init__(**kwargs)

    def provision(self, context, resource, provision_doc):
        pass

    def get(self, context, *args, **kwargs):
        #should return same resource as that in reserve method
        pass
