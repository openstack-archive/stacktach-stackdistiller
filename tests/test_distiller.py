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

import datetime

#for Python2.6 compatability.
import unittest2 as unittest

import iso8601
import jsonpath_rw
import mock
import six

from stackdistiller import distiller


class TestCondenser(object):
    def __init__(self):
        self.event_type = None
        self.message_id = None
        self.when = None
        self.traits = []

    def add_trait(self, name, trait_type, value):
        self.traits.append(distiller.Trait(name, trait_type, value))

    def add_envelope_info(self, event_type, message_id, when):
        self.event_type = event_type
        self.message_id = message_id
        self.when = when

    def get_event(self):
        return self


class DistillerTestBase(unittest.TestCase):
    def _create_test_notification(self, event_type, message_id, **kw):
        return dict(event_type=event_type,
                    message_id=message_id,
                    priority="INFO",
                    publisher_id="compute.host-1-2-3",
                    timestamp="2013-08-08 21:06:37.803826",
                    payload=kw,
                    )

    def assertIsValidEvent(self, event, notification):
        self.assertIsNot(
            None, event,
            "Notification dropped unexpectedly:"
            " %s" % str(notification))

    def assertIsNotValidEvent(self, event, notification):
        self.assertIs(
            None, event,
            "Notification NOT dropped when expected to be dropped:"
            " %s" % str(notification))

    def assertHasTrait(self, event, name, value=None, trait_type=None):
        traits = [trait for trait in event.traits if trait.name == name]
        self.assertTrue(
            len(traits) > 0,
            "Trait %s not found in event %s" % (name, event))
        trait = traits[0]
        if value is not None:
            self.assertEqual(value, trait.value)
        if trait_type is not None:
            self.assertEqual(trait_type, trait.trait_type)
            if trait_type == distiller.Datatype.int:
                self.assertIsInstance(trait.value, int)
            elif trait_type == distiller.Datatype.float:
                self.assertIsInstance(trait.value, float)
            elif trait_type == distiller.Datatype.datetime:
                self.assertIsInstance(trait.value, datetime.datetime)
            elif trait_type == distiller.Datatype.text:
                self.assertIsInstance(trait.value, six.string_types)

    def assertDoesNotHaveTrait(self, event, name):
        traits = [trait for trait in event.traits if trait.name == name]
        self.assertEqual(
            len(traits), 0,
            "Extra Trait %s found in event %s" % (name, event))

    def assertHasDefaultTraits(self, event):
        text = distiller.Datatype.text
        self.assertHasTrait(event, 'service', trait_type=text)

    def _cmp_tree(self, this, other):
        if hasattr(this, 'right') and hasattr(other, 'right'):
            return (self._cmp_tree(this.right, other.right) and
                    self._cmp_tree(this.left, other.left))
        if not hasattr(this, 'right') and not hasattr(other, 'right'):
            return this == other
        return False

    def assertPathsEqual(self, path1, path2):
        self.assertTrue(self._cmp_tree(path1, path2),
                        'JSONPaths not equivalent %s %s' % (path1, path2))


class TestTraitDefinition(DistillerTestBase):

    def setUp(self):
        super(TestTraitDefinition, self).setUp()
        self.n1 = self._create_test_notification(
            "test.thing",
            "uuid-for-notif-0001",
            instance_uuid="uuid-for-instance-0001",
            instance_id="id-for-instance-0001",
            instance_uuid2=None,
            instance_id2=None,
            host='host-1-2-3',
            bogus_date='',
            image_meta=dict(
                        disk_gb='20',
                        thing='whatzit'),
            foobar=50)

        self.test_plugin_class = mock.MagicMock(name='mock_test_plugin')
        self.test_plugin = self.test_plugin_class()
        self.test_plugin.trait_value.return_value = 'foobar'
        self.test_plugin_class.reset_mock()

        self.nothing_plugin_class = mock.MagicMock(name='mock_nothing_plugin')
        self.nothing_plugin = self.nothing_plugin_class()
        self.nothing_plugin.trait_value.return_value = None
        self.nothing_plugin_class.reset_mock()

        self.fake_plugin_map = dict(test=self.test_plugin_class,
                                    nothing=self.nothing_plugin_class)

    def test_to_trait_with_plugin(self):
        cfg = dict(type='text',
                   fields=['payload.instance_id', 'payload.instance_uuid'],
                   plugin=dict(name='test'))

        tdef = distiller.TraitDefinition('test_trait', cfg,
                                         self.fake_plugin_map)
        tname, trait_type, value = tdef.to_trait(self.n1)
        self.assertEqual('test_trait', tname)
        self.assertEqual(distiller.Datatype.text, trait_type)
        self.assertEqual('foobar', value)
        self.test_plugin_class.assert_called_once_with()
        self.test_plugin.trait_value.assert_called_once_with([
            ('payload.instance_id', 'id-for-instance-0001'),
            ('payload.instance_uuid', 'uuid-for-instance-0001')])

    def test_to_trait_null_match_with_plugin(self):
        cfg = dict(type='text',
                   fields=['payload.nothere', 'payload.bogus'],
                   plugin=dict(name='test'))

        tdef = distiller.TraitDefinition('test_trait', cfg,
                                         self.fake_plugin_map)
        tname, trait_type, value = tdef.to_trait(self.n1)
        self.assertEqual('test_trait', tname)
        self.assertEqual(distiller.Datatype.text, trait_type)
        self.assertEqual('foobar', value)
        self.test_plugin_class.assert_called_once_with()
        self.test_plugin.trait_value.assert_called_once_with([])

    def test_to_trait_with_plugin_null(self):
        cfg = dict(type='text',
                   fields=['payload.instance_id', 'payload.instance_uuid'],
                   plugin=dict(name='nothing'))

        tdef = distiller.TraitDefinition('test_trait', cfg,
                                         self.fake_plugin_map)
        t = tdef.to_trait(self.n1)
        self.assertIs(None, t)
        self.nothing_plugin_class.assert_called_once_with()
        self.nothing_plugin.trait_value.assert_called_once_with([
            ('payload.instance_id', 'id-for-instance-0001'),
            ('payload.instance_uuid', 'uuid-for-instance-0001')])

    def test_to_trait_with_plugin_with_parameters(self):
        cfg = dict(type='text',
                   fields=['payload.instance_id', 'payload.instance_uuid'],
                   plugin=dict(name='test', parameters=dict(a=1, b='foo')))

        tdef = distiller.TraitDefinition('test_trait', cfg,
                                         self.fake_plugin_map)
        tname, trait_type, value = tdef.to_trait(self.n1)
        self.assertEqual('test_trait', tname)
        self.assertEqual(distiller.Datatype.text, trait_type)
        self.assertEqual('foobar', value)
        self.test_plugin_class.assert_called_once_with(a=1, b='foo')
        self.test_plugin.trait_value.assert_called_once_with([
            ('payload.instance_id', 'id-for-instance-0001'),
            ('payload.instance_uuid', 'uuid-for-instance-0001')])

    def test_to_trait(self):
        cfg = dict(type='text', fields='payload.instance_id')
        tdef = distiller.TraitDefinition('test_trait', cfg,
                                         self.fake_plugin_map)
        t = tdef.to_trait(self.n1)
        self.assertEqual('test_trait', t.name)
        self.assertEqual(distiller.Datatype.text, t.trait_type)
        self.assertEqual('id-for-instance-0001', t.value)

        cfg = dict(type='int', fields='payload.image_meta.disk_gb')
        tdef = distiller.TraitDefinition('test_trait', cfg,
                                         self.fake_plugin_map)
        t = tdef.to_trait(self.n1)
        self.assertEqual('test_trait', t.name)
        self.assertEqual(distiller.Datatype.int, t.trait_type)
        self.assertEqual(20, t.value)

    def test_to_trait_multiple(self):
        cfg = dict(type='text', fields=['payload.instance_id',
                                        'payload.instance_uuid'])
        tdef = distiller.TraitDefinition('test_trait', cfg,
                                         self.fake_plugin_map)
        t = tdef.to_trait(self.n1)
        self.assertEqual('id-for-instance-0001', t.value)

        cfg = dict(type='text', fields=['payload.instance_uuid',
                                        'payload.instance_id'])
        tdef = distiller.TraitDefinition('test_trait', cfg,
                                         self.fake_plugin_map)
        t = tdef.to_trait(self.n1)
        self.assertEqual('uuid-for-instance-0001', t.value)

    def test_to_trait_multiple_different_nesting(self):
        cfg = dict(type='int', fields=['payload.foobar',
                   'payload.image_meta.disk_gb'])
        tdef = distiller.TraitDefinition('test_trait', cfg,
                                         self.fake_plugin_map)
        t = tdef.to_trait(self.n1)
        self.assertEqual(50, t.value)

        cfg = dict(type='int', fields=['payload.image_meta.disk_gb',
                   'payload.foobar'])
        tdef = distiller.TraitDefinition('test_trait', cfg,
                                         self.fake_plugin_map)
        t = tdef.to_trait(self.n1)
        self.assertEqual(20, t.value)

    def test_to_trait_some_null_multiple(self):
        cfg = dict(type='text', fields=['payload.instance_id2',
                                        'payload.instance_uuid'])
        tdef = distiller.TraitDefinition('test_trait', cfg,
                                         self.fake_plugin_map)
        t = tdef.to_trait(self.n1)
        self.assertEqual('uuid-for-instance-0001', t.value)

    def test_to_trait_some_missing_multiple(self):
        cfg = dict(type='text', fields=['payload.not_here_boss',
                                        'payload.instance_uuid'])
        tdef = distiller.TraitDefinition('test_trait', cfg,
                                         self.fake_plugin_map)
        t = tdef.to_trait(self.n1)
        self.assertEqual('uuid-for-instance-0001', t.value)

    def test_to_trait_missing(self):
        cfg = dict(type='text', fields='payload.not_here_boss')
        tdef = distiller.TraitDefinition('test_trait', cfg,
                                         self.fake_plugin_map)
        t = tdef.to_trait(self.n1)
        self.assertIs(None, t)

    def test_to_trait_null(self):
        cfg = dict(type='text', fields='payload.instance_id2')
        tdef = distiller.TraitDefinition('test_trait', cfg,
                                         self.fake_plugin_map)
        t = tdef.to_trait(self.n1)
        self.assertIs(None, t)

    def test_to_trait_empty_nontext(self):
        cfg = dict(type='datetime', fields='payload.bogus_date')
        tdef = distiller.TraitDefinition('test_trait', cfg,
                                         self.fake_plugin_map)
        t = tdef.to_trait(self.n1)
        self.assertIs(None, t)

    def test_to_trait_multiple_null_missing(self):
        cfg = dict(type='text', fields=['payload.not_here_boss',
                                        'payload.instance_id2'])
        tdef = distiller.TraitDefinition('test_trait', cfg,
                                         self.fake_plugin_map)
        t = tdef.to_trait(self.n1)
        self.assertIs(None, t)

    def test_missing_fields_config(self):
        self.assertRaises(distiller.EventDefinitionException,
                          distiller.TraitDefinition,
                          'bogus_trait',
                          dict(),
                          self.fake_plugin_map)

    def test_string_fields_config(self):
        cfg = dict(fields='payload.test')
        t = distiller.TraitDefinition('test_trait', cfg, self.fake_plugin_map)
        self.assertPathsEqual(t.fields, jsonpath_rw.parse('payload.test'))

    def test_list_fields_config(self):
        cfg = dict(fields=['payload.test', 'payload.other'])
        t = distiller.TraitDefinition('test_trait', cfg, self.fake_plugin_map)
        self.assertPathsEqual(
            t.fields,
            jsonpath_rw.parse('(payload.test)|(payload.other)'))

    def test_invalid_path_config(self):
        #test invalid jsonpath...
        cfg = dict(fields='payload.bogus(')
        self.assertRaises(distiller.EventDefinitionException,
                          distiller.TraitDefinition,
                          'bogus_trait',
                          cfg,
                          self.fake_plugin_map)

    def test_invalid_plugin_config(self):
        #test invalid jsonpath...
        cfg = dict(fields='payload.test', plugin=dict(bogus="true"))
        self.assertRaises(distiller.EventDefinitionException,
                          distiller.TraitDefinition,
                          'test_trait',
                          cfg,
                          self.fake_plugin_map)

    def test_unknown_plugin(self):
        #test invalid jsonpath...
        cfg = dict(fields='payload.test', plugin=dict(name='bogus'))
        self.assertRaises(distiller.EventDefinitionException,
                          distiller.TraitDefinition,
                          'test_trait',
                          cfg,
                          self.fake_plugin_map)

    def test_type_config(self):
        cfg = dict(type='text', fields='payload.test')
        t = distiller.TraitDefinition('test_trait', cfg, self.fake_plugin_map)
        self.assertEqual(distiller.Datatype.text, t.trait_type)

        cfg = dict(type='int', fields='payload.test')
        t = distiller.TraitDefinition('test_trait', cfg, self.fake_plugin_map)
        self.assertEqual(distiller.Datatype.int, t.trait_type)

        cfg = dict(type='float', fields='payload.test')
        t = distiller.TraitDefinition('test_trait', cfg, self.fake_plugin_map)
        self.assertEqual(distiller.Datatype.float, t.trait_type)

        cfg = dict(type='datetime', fields='payload.test')
        t = distiller.TraitDefinition('test_trait', cfg, self.fake_plugin_map)
        self.assertEqual(distiller.Datatype.datetime, t.trait_type)

    def test_invalid_type_config(self):
        #test invalid jsonpath...
        cfg = dict(type='bogus', fields='payload.test')
        self.assertRaises(distiller.EventDefinitionException,
                          distiller.TraitDefinition,
                          'bogus_trait',
                          cfg,
                          self.fake_plugin_map)


class TestEventDefinition(DistillerTestBase):

    def setUp(self):
        super(TestEventDefinition, self).setUp()

        self.traits_cfg = {
            'instance_id': {
                'type': 'text',
                'fields': ['payload.instance_uuid',
                           'payload.instance_id'],
            },
            'host': {
                'type': 'text',
                'fields': 'payload.host',
            },
        }

        self.test_notification1 = self._create_test_notification(
            "test.thing",
            "uuid-for-notif-0001",
            instance_id="uuid-for-instance-0001",
            host='host-1-2-3')

        self.test_notification2 = self._create_test_notification(
            "test.thing",
            "uuid-for-notif-0002",
            instance_id="uuid-for-instance-0002")

        self.test_notification3 = self._create_test_notification(
            "test.thing",
            "uuid-for-notif-0003",
            instance_id="uuid-for-instance-0003",
            host=None)
        self.fake_plugin_map = {}
        self.condenser = TestCondenser()

    def test_to_event(self):
        trait_type = distiller.Datatype.text
        cfg = dict(event_type='test.thing', traits=self.traits_cfg)
        edef = distiller.EventDefinition(cfg, self.fake_plugin_map)

        e = edef.to_event(self.test_notification1, self.condenser)
        self.assertTrue(e is self.condenser)
        self.assertEqual('test.thing', e.event_type)
        self.assertEqual(datetime.datetime(2013, 8, 8, 21, 6, 37, 803826, iso8601.iso8601.UTC),
                         e.when)

        self.assertHasDefaultTraits(e)
        self.assertHasTrait(e, 'host', value='host-1-2-3', trait_type=trait_type)
        self.assertHasTrait(e, 'instance_id',
                            value='uuid-for-instance-0001',
                            trait_type=trait_type)

    def test_to_event_missing_trait(self):
        trait_type = distiller.Datatype.text
        cfg = dict(event_type='test.thing', traits=self.traits_cfg)
        edef = distiller.EventDefinition(cfg, self.fake_plugin_map)

        e = edef.to_event(self.test_notification2, self.condenser)

        self.assertHasDefaultTraits(e)
        self.assertHasTrait(e, 'instance_id',
                            value='uuid-for-instance-0002',
                            trait_type=trait_type)
        self.assertDoesNotHaveTrait(e, 'host')

    def test_to_event_null_trait(self):
        trait_type = distiller.Datatype.text
        cfg = dict(event_type='test.thing', traits=self.traits_cfg)
        edef = distiller.EventDefinition(cfg, self.fake_plugin_map)

        e = edef.to_event(self.test_notification3, self.condenser)

        self.assertHasDefaultTraits(e)
        self.assertHasTrait(e, 'instance_id',
                            value='uuid-for-instance-0003',
                            trait_type=trait_type)
        self.assertDoesNotHaveTrait(e, 'host')

    def test_bogus_cfg_no_traits(self):
        bogus = dict(event_type='test.foo')
        self.assertRaises(distiller.EventDefinitionException,
                          distiller.EventDefinition,
                          bogus,
                          self.fake_plugin_map)

    def test_bogus_cfg_no_type(self):
        bogus = dict(traits=self.traits_cfg)
        self.assertRaises(distiller.EventDefinitionException,
                          distiller.EventDefinition,
                          bogus,
                          self.fake_plugin_map)

    def test_included_type_string(self):
        cfg = dict(event_type='test.thing', traits=self.traits_cfg)
        edef = distiller.EventDefinition(cfg, self.fake_plugin_map)
        self.assertEqual(1, len(edef._included_types))
        self.assertEqual('test.thing', edef._included_types[0])
        self.assertEqual(0, len(edef._excluded_types))
        self.assertTrue(edef.included_type('test.thing'))
        self.assertFalse(edef.excluded_type('test.thing'))
        self.assertTrue(edef.match_type('test.thing'))
        self.assertFalse(edef.match_type('random.thing'))

    def test_included_type_list(self):
        cfg = dict(event_type=['test.thing', 'other.thing'],
                   traits=self.traits_cfg)
        edef = distiller.EventDefinition(cfg, self.fake_plugin_map)
        self.assertEqual(2, len(edef._included_types))
        self.assertEqual(0, len(edef._excluded_types))
        self.assertTrue(edef.included_type('test.thing'))
        self.assertTrue(edef.included_type('other.thing'))
        self.assertFalse(edef.excluded_type('test.thing'))
        self.assertTrue(edef.match_type('test.thing'))
        self.assertTrue(edef.match_type('other.thing'))
        self.assertFalse(edef.match_type('random.thing'))

    def test_excluded_type_string(self):
        cfg = dict(event_type='!test.thing', traits=self.traits_cfg)
        edef = distiller.EventDefinition(cfg, self.fake_plugin_map)
        self.assertEqual(1, len(edef._included_types))
        self.assertEqual('*', edef._included_types[0])
        self.assertEqual('test.thing', edef._excluded_types[0])
        self.assertEqual(1, len(edef._excluded_types))
        self.assertEqual('test.thing', edef._excluded_types[0])
        self.assertTrue(edef.excluded_type('test.thing'))
        self.assertTrue(edef.included_type('random.thing'))
        self.assertFalse(edef.match_type('test.thing'))
        self.assertTrue(edef.match_type('random.thing'))

    def test_excluded_type_list(self):
        cfg = dict(event_type=['!test.thing', '!other.thing'],
                   traits=self.traits_cfg)
        edef = distiller.EventDefinition(cfg, self.fake_plugin_map)
        self.assertEqual(1, len(edef._included_types))
        self.assertEqual(2, len(edef._excluded_types))
        self.assertTrue(edef.excluded_type('test.thing'))
        self.assertTrue(edef.excluded_type('other.thing'))
        self.assertFalse(edef.excluded_type('random.thing'))
        self.assertFalse(edef.match_type('test.thing'))
        self.assertFalse(edef.match_type('other.thing'))
        self.assertTrue(edef.match_type('random.thing'))

    def test_mixed_type_list(self):
        cfg = dict(event_type=['*.thing', '!test.thing', '!other.thing'],
                   traits=self.traits_cfg)
        edef = distiller.EventDefinition(cfg, self.fake_plugin_map)
        self.assertEqual(1, len(edef._included_types))
        self.assertEqual(2, len(edef._excluded_types))
        self.assertTrue(edef.excluded_type('test.thing'))
        self.assertTrue(edef.excluded_type('other.thing'))
        self.assertFalse(edef.excluded_type('random.thing'))
        self.assertFalse(edef.match_type('test.thing'))
        self.assertFalse(edef.match_type('other.thing'))
        self.assertFalse(edef.match_type('random.whatzit'))
        self.assertTrue(edef.match_type('random.thing'))

    def test_catchall(self):
        cfg = dict(event_type=['*.thing', '!test.thing', '!other.thing'],
                   traits=self.traits_cfg)
        edef = distiller.EventDefinition(cfg, self.fake_plugin_map)
        self.assertFalse(edef.is_catchall)

        cfg = dict(event_type=['!other.thing'],
                   traits=self.traits_cfg)
        edef = distiller.EventDefinition(cfg, self.fake_plugin_map)
        self.assertFalse(edef.is_catchall)

        cfg = dict(event_type=['other.thing'],
                   traits=self.traits_cfg)
        edef = distiller.EventDefinition(cfg, self.fake_plugin_map)
        self.assertFalse(edef.is_catchall)

        cfg = dict(event_type=['*', '!other.thing'],
                   traits=self.traits_cfg)
        edef = distiller.EventDefinition(cfg, self.fake_plugin_map)
        self.assertFalse(edef.is_catchall)

        cfg = dict(event_type=['*'],
                   traits=self.traits_cfg)
        edef = distiller.EventDefinition(cfg, self.fake_plugin_map)
        self.assertTrue(edef.is_catchall)

        cfg = dict(event_type=['*', 'foo'],
                   traits=self.traits_cfg)
        edef = distiller.EventDefinition(cfg, self.fake_plugin_map)
        self.assertTrue(edef.is_catchall)

    @mock.patch('stackdistiller.distiller.utcnow')
    def test_extract_when(self, mock_utcnow):
        now = datetime.datetime.utcnow().replace(tzinfo=iso8601.iso8601.UTC)
        modified = now + datetime.timedelta(minutes=1)
        mock_utcnow.return_value = now

        body = {"timestamp": str(modified)}
        when = distiller.EventDefinition._extract_when(body)
        self.assertEqual(modified, when)

        body = {"_context_timestamp": str(modified)}
        when = distiller.EventDefinition._extract_when(body)
        self.assertEqual(modified, when)

        then = now + datetime.timedelta(hours=1)
        body = {"timestamp": str(modified), "_context_timestamp": str(then)}
        when = distiller.EventDefinition._extract_when(body)
        self.assertEqual(modified, when)

        when = distiller.EventDefinition._extract_when({})
        self.assertEqual(now, when)

    def test_default_traits(self):
        cfg = dict(event_type='test.thing', traits={})
        edef = distiller.EventDefinition(cfg, self.fake_plugin_map)
        default_traits = distiller.EventDefinition.DEFAULT_TRAITS.keys()
        traits = set(edef.traits.keys())
        for dt in default_traits:
            self.assertIn(dt, traits)
        self.assertEqual(len(distiller.EventDefinition.DEFAULT_TRAITS),
                         len(edef.traits))

    def test_traits(self):
        cfg = dict(event_type='test.thing', traits=self.traits_cfg)
        edef = distiller.EventDefinition(cfg, self.fake_plugin_map)
        default_traits = distiller.EventDefinition.DEFAULT_TRAITS.keys()
        traits = set(edef.traits.keys())
        for dt in default_traits:
            self.assertIn(dt, traits)
        self.assertIn('host', traits)
        self.assertIn('instance_id', traits)
        self.assertEqual(len(distiller.EventDefinition.DEFAULT_TRAITS) + 2,
                         len(edef.traits))


class TestDistiller(DistillerTestBase):

    def setUp(self):
        super(TestDistiller, self).setUp()

        self.valid_event_def1 = [{
            'event_type': 'compute.instance.create.*',
            'traits': {
                'instance_id': {
                    'type': 'text',
                    'fields': ['payload.instance_uuid',
                               'payload.instance_id'],
                },
                'host': {
                    'type': 'text',
                    'fields': 'payload.host',
                },
            },
        }]

        self.test_notification1 = self._create_test_notification(
            "compute.instance.create.start",
            "uuid-for-notif-0001",
            instance_id="uuid-for-instance-0001",
            host='host-1-2-3')
        self.test_notification2 = self._create_test_notification(
            "bogus.notification.from.mars",
            "uuid-for-notif-0002",
            weird='true',
            host='cydonia')
        self.fake_plugin_map = {}

    @mock.patch('stackdistiller.distiller.utcnow')
    def test_distiller_missing_keys(self, mock_utcnow):
        # test a malformed notification
        now = datetime.datetime.utcnow().replace(tzinfo=iso8601.iso8601.UTC)
        mock_utcnow.return_value = now
        c = distiller.Distiller(
            [],
            self.fake_plugin_map,
            catchall=True)
        message = {'event_type': "foo",
                   'message_id': "abc",
                   'publisher_id': "1"}
        e = c.to_event(message, TestCondenser())
        self.assertIsValidEvent(e, message)
        self.assertEqual(1, len(e.traits))
        self.assertEqual("foo", e.event_type)
        self.assertEqual(now, e.when)

    def test_distiller_with_catchall(self):
        c = distiller.Distiller(
            self.valid_event_def1,
            self.fake_plugin_map,
            catchall=True)
        self.assertEqual(2, len(c.definitions))
        e = c.to_event(self.test_notification1, TestCondenser())
        self.assertIsValidEvent(e, self.test_notification1)
        self.assertEqual(3, len(e.traits))
        self.assertHasDefaultTraits(e)
        self.assertHasTrait(e, 'instance_id')
        self.assertHasTrait(e, 'host')

        e = c.to_event(self.test_notification2, TestCondenser())
        self.assertIsValidEvent(e, self.test_notification2)
        self.assertEqual(1, len(e.traits),
            "Wrong number of traits %s: %s" % (len(e.traits), e.traits))
        self.assertHasDefaultTraits(e)
        self.assertDoesNotHaveTrait(e, 'instance_id')
        self.assertDoesNotHaveTrait(e, 'host')

    def test_distiller_without_catchall(self):
        c = distiller.Distiller(
            self.valid_event_def1,
            self.fake_plugin_map,
            catchall=False)
        self.assertEqual(1, len(c.definitions))
        e = c.to_event(self.test_notification1, TestCondenser())
        self.assertIsValidEvent(e, self.test_notification1)
        self.assertEqual(3, len(e.traits))
        self.assertHasDefaultTraits(e)
        self.assertHasTrait(e, 'instance_id')
        self.assertHasTrait(e, 'host')

        e = c.to_event(self.test_notification2, TestCondenser())
        self.assertIsNotValidEvent(e, self.test_notification2)

    def test_distiller_empty_cfg_with_catchall(self):
        c = distiller.Distiller(
            [],
            self.fake_plugin_map,
            catchall=True)
        self.assertEqual(1, len(c.definitions))
        e = c.to_event(self.test_notification1, TestCondenser())
        self.assertIsValidEvent(e, self.test_notification1)
        self.assertEqual(1, len(e.traits))
        self.assertHasDefaultTraits(e)

        e = c.to_event(self.test_notification2, TestCondenser())
        self.assertIsValidEvent(e, self.test_notification2)
        self.assertEqual(1, len(e.traits))
        self.assertHasDefaultTraits(e)

    def test_distiller_empty_cfg_without_catchall(self):
        c = distiller.Distiller(
            [],
            self.fake_plugin_map,
            catchall=False)
        self.assertEqual(0, len(c.definitions))
        e = c.to_event(self.test_notification1, TestCondenser())
        self.assertIsNotValidEvent(e, self.test_notification1)

        e = c.to_event(self.test_notification2, TestCondenser())
        self.assertIsNotValidEvent(e, self.test_notification2)

    def test_default_config(self):
        d = distiller.Distiller([], catchall=True)
        self.assertEqual(1, len(d.definitions))
        self.assertTrue(d.definitions[0].is_catchall)

        d = distiller.Distiller([], catchall=False)
        self.assertEqual(0, len(d.definitions))
