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

import abc
import six


@six.add_metaclass(abc.ABCMeta)
class CondenserBase(object):
    """Base class for Condenser objects that collect data extracted from a
       Notification by the Distiller, and format it into a usefull datastructure.

       A simple Condenser may just colect all the traits received into a dictionary.
       More complex ones may build collections of application or ORM model objects,
       or XML document trees.

       Condensers also have hooks for verification logic, to check that all needed
       traits are present."""

    def __init__(self, **kw):
        """Setup the condenser. A new instance of the condenser is passed to the
        distiller for each notification extracted.

        :param kw: keyword parameters for condenser.

        """
        super(CondenserBase, self).__init__()

    @abc.abstractmethod
    def add_trait(self, name, trait_type, value):
        """Add a trait to the Event datastructure being built by this
        condenser. The distiller will call this for each extracted trait.

        :param name: (string) name of the trait
        :param trait_type: (distiller.Datatype) data type of the trait.
        :param value: Value of the trait (of datatype indicated by trait_type)

        """

    @abc.abstractmethod
    def add_envelope_info(self, event_type, message_id, when):
        """Add the metadata for this event, extracted from the notification's
        envelope. The distiller will call this once.

        :param event_type: (string) Type of event, as a dotted string such as
                           "compute.instance.update".
        :param message_id: (string) UUID of notification.
        :param when: (datetime) Timestamp of notification from source system.

        """

    @abc.abstractmethod
    def get_event(self):
        """Return the Event datastructure constructed by this condenser."""

    @abc.abstractmethod
    def clear(self):
        """Clear condenser state."""

    def validate(self):
        """Check Event against whatever validation logic this condenser may have

        :returns:   (bool) True if valid.

        """
        return True


class DictionaryCondenser(CondenserBase):
    """Return event data as a simple python dictionary"""
    def __init__(self, **kw):
        self.clear()
        super(DictionaryCondenser, self).__init__(**kw)

    def get_event(self):
        return self.event

    def clear(self):
        self.event = dict()

    def add_envelope_info(self, event_type, message_id, when):
        self.event['event_type'] = event_type
        self.event['message_id'] = message_id
        self.event['when'] = when

    def add_trait(self, name, trait_type, value):
        self.event[name] = value
