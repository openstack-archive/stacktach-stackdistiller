#!/usr/bin/env python
# -*- encoding: utf-8 -*-
#
# Copyright © 2014 Rackspace Hosting.
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

"""Command line tool help you debug your distiller event definitions.

Feed it a list of test notifications in json format, and it will show
you what events will be generated.
"""

import argparse
import json
import sys

from stackdistiller import distiller
from stackdistiller import condenser


class TestCondenser(condenser.CondenserBase):
    def __init__(self):
        self.clear()

    def add_trait(self, name, trait_type, value):
        self.traits.append(distiller.Trait(name, trait_type, value))

    def add_envelope_info(self, event_type, message_id, when):
        self.event_type = event_type
        self.message_id = message_id
        self.when = when

    def get_event(self):
        return self

    def clear(self):
        self.event_type = None
        self.message_id = None
        self.when = None
        self.traits = []

    def __str__(self):
        text = ["Event: %s (id: %s) at %s" % (self.event_type,
                                              self.message_id,
                                              self.when)]
        for trait in sorted(self.traits):
            text.append("    Trait: name: %s, type: %s, value: %s" % trait)
        text.append('')
        return "\n".join(text)


def test_data(args):
    if not args.test_data:
        n = json.load(sys.stdin)
        if args.list:
            for notif in n:
                yield notif
        else:
            yield n
    else:
        for f in args.test_data:
            with open(f, 'r') as data:
                n = json.load(data)
            if args.list:
                for notif in n:
                    yield notif
            else:
                yield n


parser = argparse.ArgumentParser(description="Test Distiller configuration")
parser.add_argument('-c', '--config',
                    default='event_definitions.yaml',
                    help='Name of event definitions file '
                         'to test (Default: %(default)s)')
parser.add_argument('-l', '--list', action='store_true',
                    help='Test data files contain JSON list of notifications.'
                    ' (By default data files should contain a single '
                    'notification.)')
parser.add_argument('-d', '--add_default_definition', action='store_true',
                    help='Add default event definition. Normally, '
                    'notifications are dropped if there is no event '
                    'definition for their event_type. Setting this adds a '
                    '"catchall" that converts unknown notifications to Events'
                    ' with a few basic traits.')
parser.add_argument('-o', '--output',  type=argparse.FileType('w'),
                    default=sys.stdout, help="Output file. Default stdout")
parser.add_argument('test_data', nargs='*', metavar='JSON_FILE',
                    help="Test notifications in JSON format. Defaults to stdin")
args = parser.parse_args()


config = distiller.load_config(args.config)

out = args.output
out.write("Definitions file: %s\n" % args.config)
notifications = test_data(args)

dist = distiller.Distiller(config, catchall=args.add_default_definition)
nct = 0
drops = 0
cond = TestCondenser()
for notification in notifications:
    cond.clear()
    nct +=1
    if dist.to_event(notification, cond) is None:
        out.write("Dropped notification: %s\n" %
                  notification['message_id'])
        drops += 1
    else:
        event = cond.get_event()
        out.write(str(event))
        out.write("--\n")

out.write("Notifications tested: %s (%s dropped)\n" % (nct, drops))
