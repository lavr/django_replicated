# encoding: utf-8
import time
from django_replicated.failover_router import MasterFailoverReplicationRouter
from django_replicated.failover_router import FailoverThread


def test_thread_with_master_choose_cycle():
    """
    Check master selection
    """
    from django.conf import settings
    settings.DATABASE_SLAVES = ['slave1', 'slave2']

    r = MasterFailoverReplicationRouter(run_thread=False)
    assert len(r.SLAVES) == 2

    t = r.checker_cls(router=r, check_master=True)

    t.db_is_alive = lambda alias: alias == 'slave1'
    t.db_is_read_only = lambda alias: alias != 'slave1'
    t.check()
    t.check()

    assert r.SLAVES == []
    assert r.master == 'slave1'

    t.db_is_alive = lambda alias: alias == 'slave1'
    t.db_is_read_only = lambda alias: alias != 'default'
    t.check()
    t.check()
    t.check()
    assert r.master == 'default'
    assert r.SLAVES == ['slave1']