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

import copy


class DictableObject(object):
    def __init__(self, **kwargs):
        if kwargs:
            for (k, v) in kwargs.items():
                setattr(self, k, v)

    def to_dict(self):
        myself = {}
        for (k, v) in self.__dict__.items():
            if k.startswith("_"):
                continue
            if isinstance(v, DictableObject):
                v = v.to_dict()
            myself[k] = v
        return myself

    def __deepcopy__(self, obj):
        output = self.__class__()
        obj[id(self)] = output
        for k, v in self.__dict__.items():
            setattr(output, k, copy.deepcopy(v, obj))
        return output

    def __getattr__(self, name):
        # Be tolerant of fields not existing...
        return None

    def __getitem__(self, key):
        return getattr(self, key, None)
