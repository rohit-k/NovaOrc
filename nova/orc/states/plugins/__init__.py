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
from nova.db import base


class Driver(object):
    """Abstract base class for reserving and releasing resources"""

    def __init__(self, **kwargs):
        db_base = base.Base()
        self.db = db_base.db
        self.compute_rpcapi = kwargs.get("compute_rpcapi")
        self.conductor_api = kwargs.get("conductor_api")
        self.network_api = kwargs.get("network_api")
        self.scheduler_rpcapi = kwargs.get("scheduler_rpcapi")
        self.volume_api = kwargs.get("volume_api")

    def _instance_update(self, context, instance_uuid, **kwargs):
        """Update an instance in the database using kwargs as value."""

        instance_ref = self.conductor_api.instance_update(context,
            instance_uuid, **kwargs)

        return instance_ref


class ReservationDriver(Driver):
    """Abstract base class for reserving and releasing resources"""

    def __init__(self, **kwargs):
        super(ReservationDriver, self).__init__(**kwargs)

    @abc.abstractmethod
    def reserve(self, context, *args, **kwargs):
        raise NotImplementedError()

    @abc.abstractmethod
    def unreserve(self, context, *args, **kwargs):
        raise NotImplementedError()

    @abc.abstractmethod
    def get(self, context, *args, **kwargs):
        raise NotImplementedError()


class ProvisioningDriver(Driver):
    """Abstract base class for provisioning resources"""

    def __init__(self, **kwargs):
        super(ProvisioningDriver, self).__init__(**kwargs)

    @abc.abstractmethod
    def provision(self, context, *args, **kwargs):
        raise NotImplementedError()

    @abc.abstractmethod
    def get(self, context, *args, **kwargs):
        raise NotImplementedError()
