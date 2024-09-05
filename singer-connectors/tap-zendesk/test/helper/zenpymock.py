from collections import defaultdict
from time import sleep

from zenpy import Zenpy, TicketApi
import random

from zenpy.lib.api_objects import BaseObject
from tap_zendesk import get_default_config

from tap_zendesk import LOGGER


class TicketApiMock(TicketApi):
    pass


class DictMock(BaseObject):
    def __init__(self, dict):
        for k, v in dict.items():
            setattr(self, k, v)


class ZenpyMock(Zenpy):
    def __init__(self, n_tickets=1000, p_sleep=None, p_failure=None, *args, **kwargs):
        super(ZenpyMock, self).__init__(*args, **kwargs)
        self.internal_config = get_default_config()

        tickets = self.tickets

        self.n_tickets = n_tickets
        self.p_sleep = p_sleep
        self.p_failure = p_failure

        tickets.incremental = self.incremental
        tickets.comments = self.comments
        tickets.audits = self.audits
        tickets.metrics = self.metrics

        self.last_generated_ticket_ts = 1573589045
        self.last_generated_ticket_id = 10335662

        self.generated_tickets = []
        self.generated_comments = {}
        self.generated_audits = {}
        self.generated_metrics = {}

    def incremental(self, start_time):
        for i in range(self.n_tickets):
            yield self._store_fake_ticket()

    def comments(self, ticket):
        if self.p_sleep and random.random() < self.p_sleep:
            sleep(1)

        if self.p_failure and random.random() < self.p_failure:
            raise RuntimeError

        return self.generated_comments[ticket]

    def audits(self, ticket=None, **kwargs):
        if self.p_sleep and random.random() < self.p_sleep:
            sleep(1)

        if self.p_failure and random.random() < self.p_failure:
            raise RuntimeError

        return self.generated_audits[ticket]

    def metrics(self, ticket):
        if self.p_sleep and random.random() < self.p_sleep:
            sleep(1)

        if self.p_failure and random.random() < self.p_failure:
            raise RuntimeError

        return self.generated_metrics[ticket]

    def _store_fake_ticket(self):
        new_ts = self.last_generated_ticket_ts + random.randint(1, 1000)
        new_id = self.last_generated_ticket_id + 1

        new_ticket = self._generate_fake_ticket(new_id, new_ts)
        self.generated_tickets.append(new_ticket)
        self.generated_comments[new_id] = [self._generate_fake_comment(new_id) for i in range(random.randint(0, 10))]
        self.generated_audits[new_id] = [self._generate_fake_audit(new_id) for i in range(random.randint(0, 10))]
        self.generated_metrics[new_id] = [self._generate_fake_metric(new_id) for i in range(random.randint(0, 10))]

        self.last_generated_ticket_id = new_id
        self.last_generated_ticket_ts = new_ts

        return new_ticket


    def _generate_fake_ticket(self, id, timestamp):
        d = {
            'id': id,
            'generated_timestamp': timestamp,
            'fields': []
        }

        return DictMock(d)

    def _generate_fake_comment(self, ticket_id):
        d = {
            'id': random.randint(0, 10000000),
            'ticket_id': ticket_id
        }

        return DictMock(d)

    def _generate_fake_audit(self, ticket_id):
        d = {
            'id': random.randint(0, 10000000),
            'ticket_id': ticket_id
        }

        return d

    def _generate_fake_metric(self, ticket_id):
        d = {
            'id': random.randint(0, 10000000),
            'ticket_id': ticket_id
        }

        return d
