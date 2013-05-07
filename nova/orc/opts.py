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
Global orchestration config options
"""

from oslo.config import cfg


orchestration_opts = [
    cfg.StrOpt('topic',
                default='orc',
                help='the topic orchestration nodes listen on'),
    cfg.StrOpt('manager',
                default='nova.orc.manager.OrchestrationManager',
                help='Full class name of the Manager for orchestration'),
    cfg.StrOpt('worfklow_persistent_backend_driver',
                default='nova.orc.backends.sql.SqlDriver',
                help='Default driver for storing workflow state data'),
    cfg.StrOpt('workflow_messaging_driver',
                default='nova.orc.messaging.rpcapi.OrchestrationAPI',
                help='Default driver for passing workflow specific messages'),
    cfg.StrOpt('zookeeper_queue_path',
        default='/resources',
        help='The queue path to use in Zookeeper'),
]

orchestration_group = cfg.OptGroup(name='orchestration',
                                   title='Orchestration Options')
CONF = cfg.CONF
CONF.register_group(orchestration_group)
CONF.register_opts(orchestration_opts, orchestration_group)
