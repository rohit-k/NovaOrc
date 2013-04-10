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

import abc

from nova.compute import rpcapi as compute_rpcapi
from nova import conductor
from nova.db import base
from nova import network
from nova.scheduler import rpcapi as scheduler_rpcapi

# Useful to know when other states
# are being activated and finishing...
#
# TODO(harlowja): likely these should be
# properties of the state (at least the non-basic ones)
STARTING = 0
COMPLETED = 2
POLICIES_VALIDATED = 3
IMAGE_VALIDATED = 4
REQUEST_VALIDATED = 5
CREATED_DB_ENTRY = 6


class State(base.Base):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        super(State, self).__init__()
        self.name = self.__class__.__name__

    def __str__(self):
        return "State: %s" % (self.name)

    @abc.abstractmethod
    def apply(self, context, *args, **kwargs):
        raise NotImplementedError()

    def revert(self, context, result, chain, excp, cause):
        pass

    # Used to notify state when another the overall progress
    # of another state (or this state) changes
    def notify(self, context, state, performer_name, chain, *args, **kwargs):
        pass


class ResourceUsingState(State):
    def __init__(self, **kwargs):
        super(ResourceUsingState, self).__init__()
        self.compute_rpcapi = kwargs.get("compute_rpcapi")
        self.conductor_api = kwargs.get("conductor_api")
        self.network_api = kwargs.get("network_api")
        self.scheduler_rpcapi = kwargs.get("scheduler_rpcapi")
        self.volume_api = kwargs.get("volume_api")
