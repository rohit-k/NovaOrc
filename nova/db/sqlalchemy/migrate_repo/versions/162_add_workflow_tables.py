# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2012 OpenStack Foundation
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

try:
    from collections import OrderedDict
except IOError:
    from ordereddict import OrderedDict

from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table

from nova.openstack.common import log as logging

LOG = logging.getLogger(__name__)


def _populate_workflow(workflows_table):
    workflows = OrderedDict()

    workflows['reserve_instances'] = dict(workflow_type_id=1,
                                          next_workflow_id=2)
    workflows['reserve_networks'] = dict(workflow_type_id=1,
                                         next_workflow_id=3)
    workflows['reserve_volumes'] = dict(workflow_type_id=1,
                                        next_workflow_id=4)
    workflows['provision_networks'] = dict(workflow_type_id=1,
                                           next_workflow_id=5)
    workflows['provision_volumes'] = dict(workflow_type_id=1,
                                          next_workflow_id=6)
    workflows['provision_instances'] = dict(workflow_type_id=1,
                                            next_workflow_id=0)
    #Todo(rohit): Add reconciliation chains

    try:
        insert = workflows_table.insert()
        for name, value in workflows.iteritems():
            insert.execute({'name': name,
                           'workflow_type_id': value["workflow_type_id"],
                           'deleted': False})
        for (count, value) in enumerate(workflows.values(), start=1):
            update = workflows_table.update(workflows_table.c.id == count)
            update.values(next_workflow_id=value['next_workflow_id']).execute()

    except Exception:
        LOG.info(repr(workflows_table))
        LOG.exception(_('Exception while seeding workflow table'))
        raise


def _populate_workflow_types(workflow_types_table):
    default_workflow_types = ['create', 'delete', 'reboot', 'snapshot',
                              'resize', 'live_migrate']

    try:
        i = workflow_types_table.insert()
        for name in default_workflow_types:
            i.execute({'name': name, 'deleted': False})

    except Exception:
        LOG.info(repr(workflow_types_table))
        LOG.exception(_('Exception while seeding workflow_types table'))
        raise


def upgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine

    workflow = Table('workflow', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False),
        Column('name', String(length=255), nullable=False),
        Column('workflow_type_id', Integer, ForeignKey('workflow_types.id'),
               nullable=False),
        Column('next_workflow_id', Integer),
        mysql_engine='InnoDB',
        mysql_charset='utf8',
    )

    workflow_types = Table('workflow_types', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False,
            autoincrement=True),
        Column('name', String(length=255)),
        mysql_engine='InnoDB',
        mysql_charset='utf8',
    )

    workflow_request = Table('workflow_request', meta,
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
        Column('deleted_at', DateTime),
        Column('deleted', Boolean),
        Column('id', Integer, primary_key=True, nullable=False,
                autoincrement=True),
        Column('status', String(length=36)),
        Column('workflow_type_id', Integer, ForeignKey('workflow_types.id'),
                nullable=False),
        Column('current_workflow_id', Integer),
        Column('request_id', String(length=255), nullable=False),
        mysql_engine='InnoDB',
        mysql_charset='utf8',
    )

    tables = [workflow_types, workflow, workflow_request]

    for table in tables:
        try:
            table.create()
        except Exception:
            LOG.info(repr(table))
            LOG.exception(_('Exception while creating table.'))
            raise

    Index('workflow_request_reqid_idx',
        workflow_request.c.request_id).create(migrate_engine)

    _populate_workflow_types(workflow_types)
    _populate_workflow(workflow)


def downgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine

    Table('workflow', meta, autoload=True).drop()
    Table('workflow_types', meta, autoload=True).drop()
    Table('workflow_request', meta, autoload=True).drop()
