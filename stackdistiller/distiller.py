# -*- encoding: utf-8 -*-
#
# Copyright © 2013 Rackspace Hosting.
#
# Author: Monsyne Dragon <mdragon@rackspace.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import collections
import datetime
import fnmatch
import logging
import os

from enum import Enum
import iso8601
import jsonpath_rw
import six
import yaml

from stackdistiller.condenser import DictionaryCondenser
from stackdistiller.trait_plugins import DEFAULT_PLUGINMAP


logger = logging.getLogger(__name__)


def utcnow():
    # defined here so the call can be mocked out in unittests.
    dt = datetime.datetime.utcnow()
    return dt.replace(tzinfo=iso8601.iso8601.UTC)


def convert_datetime(value):
    value = iso8601.parse_date(value)
    tz = iso8601.iso8601.UTC
    return value.astimezone(tz)


def load_config(filename):
    """Load the event definitions from yaml config file."""
    logger.debug("Event Definitions configuration file: %s", filename)

    with open(filename, 'r') as cf:
        config = cf.read()

    try:
        events_config = yaml.safe_load(config)
    except yaml.YAMLError as err:
        if hasattr(err, 'problem_mark'):
            mark = err.problem_mark
            errmsg = ("Invalid YAML syntax in Event Definitions file "
                        "%(file)s at line: %(line)s, column: %(column)s."
                      % dict(file=filename,
                             line=mark.line + 1,
                             column=mark.column + 1))
        else:
            errmsg = ("YAML error reading Event Definitions file "
                        "%(file)s"
                      % dict(file=filename))
        logger.error(errmsg)
        raise

    logger.info("Event Definitions: %s", events_config)
    return events_config


class EventDefinitionException(Exception):
    def __init__(self, message, definition_cfg):
        super(EventDefinitionException, self).__init__(message)
        self.definition_cfg = definition_cfg

    def __str__(self):
        return '%s %s: %s' % (self.__class__.__name__,
                              self.definition_cfg, self.message)


class Datatype(Enum):
    text = (1, str)
    int = (2, int)
    float = (3, float)
    datetime = (4, convert_datetime)

    def convert(self, value):
        f = self.value[1]
        return f(value)


Trait = collections.namedtuple('Trait', ('name', 'trait_type', 'value'))


class TraitDefinition(object):

    def __init__(self, name, trait_cfg, plugin_map):
        self.cfg = trait_cfg
        self.name = name

        type_name = trait_cfg.get('type', 'text')

        if 'plugin' in trait_cfg:
            plugin_cfg = trait_cfg['plugin']
            if isinstance(plugin_cfg, six.string_types):
                plugin_name = plugin_cfg
                plugin_params = {}
            else:
                try:
                    plugin_name = plugin_cfg['name']
                except KeyError:
                    raise EventDefinitionException(
                        'Plugin specified, but no plugin name supplied for '
                          'trait %s' % name, self.cfg)
                plugin_params = plugin_cfg.get('parameters')
                if plugin_params is None:
                    plugin_params = {}
            try:
                plugin_class = plugin_map[plugin_name]
            except KeyError:
                raise EventDefinitionException(
                    'No plugin named %(plugin)s available for '
                      'trait %(trait)s' % dict(plugin=plugin_name,
                                                trait=name), self.cfg)
            self.plugin = plugin_class(**plugin_params)
        else:
            self.plugin = None

        if 'fields' not in trait_cfg:
            raise EventDefinitionException(
                "Required field in trait definition not specified: "
                  "'%s'" % 'fields',
                self.cfg)

        fields = trait_cfg['fields']
        if not isinstance(fields, six.string_types):
            # NOTE(mdragon): if not a string, we assume a list.
            if len(fields) == 1:
                fields = fields[0]
            else:
                fields = '|'.join('(%s)' % path for path in fields)
        try:
            self.fields = jsonpath_rw.parse(fields)
        except Exception as e:
            raise EventDefinitionException(
                "Parse error in JSONPath specification "
                  "'%(jsonpath)s' for %(trait)s: %(err)s"
                % dict(jsonpath=fields, trait=name, err=e), self.cfg)
        try:
            self.trait_type = Datatype[type_name]
        except KeyError:
            raise EventDefinitionException(
                "Invalid trait type '%(type)s' for trait %(trait)s"
                % dict(type=type_name, trait=name), self.cfg)

    def _get_path(self, match):
        if match.context is not None:
            for path_element in self._get_path(match.context):
                yield path_element
            yield str(match.path)

    def to_trait(self, notification_body):
        values = [match for match in self.fields.find(notification_body)
                  if match.value is not None]

        if self.plugin is not None:
            value_map = [('.'.join(self._get_path(match)), match.value) for
                         match in values]
            value = self.plugin.trait_value(value_map)
        else:
            value = values[0].value if values else None

        if value is None:
            return None

        # NOTE(mdragon): some openstack projects (mostly Nova) emit ''
        # for null fields for things like dates.
        if self.trait_type != Datatype.text and value == '':
            return None

        value = self.trait_type.convert(value)
        return Trait(self.name, self.trait_type, value)


class EventDefinition(object):

    DEFAULT_TRAITS = dict(
        service=dict(type='text', fields='publisher_id'),
        request_id=dict(type='text', fields='_context_request_id'),
        tenant_id=dict(type='text', fields=['payload.tenant_id',
                                            '_context_tenant']),
    )

    def __init__(self, definition_cfg, trait_plugin_map):
        self._included_types = []
        self._excluded_types = []
        self.traits = dict()
        self.cfg = definition_cfg

        try:
            event_type = definition_cfg['event_type']
            traits = definition_cfg['traits']
        except KeyError as err:
            raise EventDefinitionException(
                "Required field %s not specified" % err.args[0], self.cfg)

        if isinstance(event_type, six.string_types):
            event_type = [event_type]

        for t in event_type:
            if t.startswith('!'):
                self._excluded_types.append(t[1:])
            else:
                self._included_types.append(t)

        if self._excluded_types and not self._included_types:
            self._included_types.append('*')

        for trait_name in self.DEFAULT_TRAITS:
            self.traits[trait_name] = TraitDefinition(
                trait_name,
                self.DEFAULT_TRAITS[trait_name],
                trait_plugin_map)
        for trait_name in traits:
            self.traits[trait_name] = TraitDefinition(
                trait_name,
                traits[trait_name],
                trait_plugin_map)

    def included_type(self, event_type):
        for t in self._included_types:
            if fnmatch.fnmatch(event_type, t):
                return True
        return False

    def excluded_type(self, event_type):
        for t in self._excluded_types:
            if fnmatch.fnmatch(event_type, t):
                return True
        return False

    def match_type(self, event_type):
        return (self.included_type(event_type)
                and not self.excluded_type(event_type))

    @property
    def is_catchall(self):
        return '*' in self._included_types and not self._excluded_types

    @staticmethod
    def _extract_when(body):
        """Extract the generated datetime from the notification.
        """
        # NOTE: I am keeping the logic the same as it was in openstack
        # code, However, *ALL* notifications should have a 'timestamp'
        # field, it's part of the notification envelope spec. If this was
        # put here because some openstack project is generating notifications
        # without a timestamp, then that needs to be filed as a bug with the
        # offending project (mdragon)
        when = body.get('timestamp', body.get('_context_timestamp'))
        if when:
            return Datatype.datetime.convert(when)
        return utcnow()

    def to_event(self, notification_body, condenser):
        event_type = notification_body['event_type']
        message_id = notification_body['message_id']
        when = self._extract_when(notification_body)

        condenser.add_envelope_info(event_type, message_id, when)
        for t in self.traits:
            trait_info = self.traits[t].to_trait(notification_body)
            # Only accept non-None value traits ...
            if trait_info is not None:
                condenser.add_trait(*trait_info)
        return condenser


class Distiller(object):
    """Distiller

    The Distiller extracts relevent information from an OpenStack Notification,
    with optional minor data massaging, and hands the information to a
    Condenser object, which formats it into an Event which only contains
    information you need, in a format relevent to your application.

    The extraction is handled according to event definitions in a config file.

    :param events_config:       event-definitions configuration deserializerd
                                from the YAML config-file.
    :param trait_plugin_map:    Dictionary, or dictionary-like object mapping
                                names to plugin classes. Defaults to default
                                map of builtin plugins.
    :param catchall:            Boolean. Add basic event definition to cover
                                any notifications not otherwise described by
                                the loaded config. The basic event definion
                                only has envelope metadata, and a few really
                                basic traits. Defaults to False.

    """

    def __init__(self, events_config, trait_plugin_map=None, catchall=False):
        if trait_plugin_map is None:
            trait_plugin_map = DEFAULT_PLUGINMAP
        self.definitions = [
            EventDefinition(event_def, trait_plugin_map)
            for event_def in reversed(events_config)]
        self.catchall = catchall
        if catchall and not any(d.is_catchall for d in self.definitions):
            event_def = dict(event_type='*', traits={})
            self.definitions.append(EventDefinition(event_def,
                                                    trait_plugin_map))

    def to_event(self, notification_body, condenser=None):
        if condenser is None:
            condenser = DictionaryCondenser()
        event_type = notification_body['event_type']
        message_id = notification_body['message_id']
        edef = None
        for d in self.definitions:
            if d.match_type(event_type):
                edef = d
                break

        if edef is None:
            msg = ('Dropping Notification %(type)s (uuid:%(msgid)s)'
                   % dict(type=event_type, msgid=message_id))
            if self.catchall:
                # If catchall is True, this should
                # never happen. (mdragon)
                logger.error(msg)
            else:
                logger.debug(msg)
            return None

        return edef.to_event(notification_body, condenser)
