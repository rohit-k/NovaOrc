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

from migrate import ForeignKeyConstraint
from sqlalchemy import Boolean, Column, DateTime, Index, Integer
from sqlalchemy import MetaData, String, Table, Text
from sqlalchemy import dialects

from nova.openstack.common import log as logging

LOG = logging.getLogger(__name__)


def MediumText():
    return Text().with_variant(dialects.mysql.MEDIUMTEXT(), 'mysql')


def upgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine

    resource_tracker = Table('resource_tracker', meta,
                             Column('created_at', DateTime),
                             Column('updated_at', DateTime),
                             Column('deleted_at', DateTime),
                             Column('deleted', Boolean),
                             Column('id', Integer, primary_key=True,
                                    autoincrement=True),
                             Column('request_uuid', String(length=255),
                                    nullable=False, unique=True),
                             Column('tracking_id', String(length=255),
                                    unique=True),
                             Column('status', String(length=255)),
                             mysql_engine='InnoDB',
                             mysql_charset='utf8',
                       )

    resource_tracker_action = Table('resource_tracker_action', meta,
                                Column('created_at', DateTime),
                                Column('updated_at', DateTime),
                                Column('deleted_at', DateTime),
                                Column('deleted', Boolean),
                                Column('tracking_id', String(length=255),
                                       primary_key=True, nullable=False),
                                Column('action_performed', String(length=255),
                                       primary_key=True, nullable=False),
                                Column('action_result', MediumText()),
                                mysql_engine='InnoDB',
                                mysql_charset='utf8',
                             )

    tables = [resource_tracker_action, resource_tracker]

    for table in tables:
        try:
            table.create()
        except Exception:
            LOG.info(repr(table))
            LOG.exception(_('Exception while creating table.'))
            raise

    Index('resource_tracker_trackingid_idx',
        resource_tracker.c.tracking_id).create(migrate_engine)

    Index('resource_tracker_request_uuid_idx',
        resource_tracker.c.request_uuid).create(migrate_engine)

    fkeys = [
             [[resource_tracker_action.c.tracking_id],
               [resource_tracker.c.tracking_id],
                'resource_tracking_id_fkey']
            ]

    for fkey_pair in fkeys:
        if migrate_engine.name == 'mysql':
            # For MySQL we name our fkeys explicitly so they match Folsom
            fkey = ForeignKeyConstraint(columns=fkey_pair[0],
                                   refcolumns=fkey_pair[1],
                                   name=fkey_pair[2])
            fkey.create()
        elif migrate_engine.name == 'postgresql':
            # PostgreSQL names things like it wants (correct and compatible!)
            fkey = ForeignKeyConstraint(columns=fkey_pair[0],
                                   refcolumns=fkey_pair[1])
            fkey.create()


def downgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine
    Table('resource_tracker_action', meta, autoload=True).drop()
    Table('resource_tracker', meta, autoload=True).drop()
