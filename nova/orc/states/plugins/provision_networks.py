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

from oslo.config import cfg

from nova.openstack.common import log as logging
from nova.orc import states
from nova.orc import utils as orc_utils


CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class ProvisionNetworksDriver(states.ResourceUsingState):
    """Driver that implements network provisioning"""

    def __init__(self, **kwargs):
        super(ProvisionNetworksDriver, self).__init__(**kwargs)

    def apply(self, context, resource, provision_doc, **kwargs):
        return orc_utils.DictableObject()

    def revert(self, context, result, chain, excp, cause):
        pass
