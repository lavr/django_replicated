# -*- coding:utf-8 -*-
import random
import time
import logging
from threading import local, Thread, RLock, Event

from django.conf import settings

from .router import ReplicationRouter
from .db_utils import db_is_alive, db_is_not_read_only

logger = logging.getLogger('replicated.router')


class FailoverThread(Thread):

    def __init__(self, router, check_interval=None, check_master=False):
        self.router = router
        self.check_interval = check_interval
        self.do_check_master = check_master
        self._stop = Event()
        Thread.__init__(self)

    def join(self, timeout=None):
        self._stop.set()
        Thread.join(self, timeout=timeout)

    def run(self):
        while not self._stop.isSet():
            self.check()
            time.sleep(self.check_interval)

    def check(self):
        self.check_slaves()
        if self.do_check_master:
            self.check_master()

    def check_slaves(self):
        """
        Check if slaves alive.
        Deactivate dead slaves.
        Activate previously deactivated slaves.
        """
        for alias in self.router.SLAVES:
            logger.debug('[thread] Check database %s still alive', alias)
            if not self.db_is_alive(alias):
                self.router.deactivate_slave(alias)

        for alias in self.router.deactivated_slaves:
            logger.debug('[thread] Check database %s alive again', alias)
            if self.db_is_alive(alias):
                self.router.activate_slave(alias)

    def check_master(self):
        """
        Check master and deactivate it.
        Deactivated master is a process time economy.
        """

        alias = self.router.DEFAULT_DB_ALIAS
        r = self.router
        if r.master:
            logger.debug('[thread] Check master %s still alive', alias)
            if not self.db_is_alive(alias):
                r.deactivate_master()
        else:
            logger.debug('[thread] Check master %s alive again', alias)
            if self.db_is_alive(alias):
                r.activate_master()

    def db_is_alive(self, alias, **kwargs):
        return db_is_alive(alias, **kwargs)


class FailoverReplicationRouter(ReplicationRouter):

    checker_cls = FailoverThread

    def __init__(self, run_thread=None, checker_cls=None):
        super(FailoverReplicationRouter, self).__init__()
        self._master = self.DEFAULT_DB_ALIAS
        self.deactivated_slaves = []
        self.rlock = RLock()
        self.thread = None
        self.checker_cls = checker_cls or self.checker_cls

        if run_thread is not None:
            _run = run_thread
        else:
            _run = getattr(settings, 'DATABASE_ASYNC_CHECK', False)

        if _run:
            self.start_thread()

    def start_thread(self, force=False):
        do_check_master = getattr(settings, 'DATABASE_CHECK_MASTER', False)
        if force or self.SLAVES or do_check_master:
            return self._start_thread(cls=self.checker_cls,
                              check_interval=getattr(settings, 'DATABASE_ASYNC_CHECK_INTERVAL', 5),
                              check_master=do_check_master)

    def _start_thread(self, cls, **kw):
        cls = cls or self.checker_cls
        self.thread = cls(router=self, **kw)
        self.thread.start()

    def stop_thread(self):
        if self.thread:
            logger.debug("stopping checker thread")
            self.thread.join(timeout=self.thread.check_interval*2)

    @property
    def master(self):
        return self._master

    @master.setter
    def master(self, alias):
        self._master = alias

    def deactivate_slave(self, alias):
        with self.rlock:
            if alias not in self.deactivated_slaves:
                self.deactivated_slaves.append(alias)
            if alias in self.SLAVES:
                logger.info("Deactivate slave '%s'", alias)
                self.SLAVES.remove(alias)

    def activate_slave(self, alias):
        with self.rlock:
            if alias not in self.SLAVES:
                logger.info("Activate slave '%s'", alias)
                self.SLAVES.append(alias)
            if alias in self.deactivated_slaves:
                self.deactivated_slaves.remove(alias)

    def deactivate_master(self):
        with self.rlock:
            m = self.master
            if m:
                logger.info("Deactivate master '%s'", m)
                self.master = None

    def activate_master(self):
        with self.rlock:
            if not self.master:
                logger.info("Activate master '%s'", self.DEFAULT_DB_ALIAS)
                self.master = self.DEFAULT_DB_ALIAS

    def db_for_write(self, *a, **kw):
        r = super(FailoverReplicationRouter, self).db_for_write(*a, **kw)
        if (r == self.DEFAULT_DB_ALIAS) and self.master is None:
            return None

    def db_for_read(self, *a, **kw):
        r = super(FailoverReplicationRouter, self).db_for_read(*a, **kw)
        if (r == self.DEFAULT_DB_ALIAS) and self.master is None:
            return None

    def __del__(self):
        self.stop_thread()


class MasterFailoverThread(FailoverThread):

    def __init__(self, *args, **kwargs):
        super(MasterFailoverThread, self).__init__(*args, **kwargs)

    def db_is_read_only(self, alias, **kwargs):
        return not db_is_not_read_only(alias, **kwargs)

    def check_master(self):
        if self.db_is_read_only(self.router.MASTER):
            # If master is read only ...
            for alias in self.router.SLAVES:
                if not self.db_is_read_only(alias):
                    # ... promote one of read-write slaves to master
                    self.router.master = alias
                    break


class MasterFailoverReplicationRouter(FailoverReplicationRouter):
    """
    Promote one of read-write slave to master if current master is in read-only mode.

    WARNING: this class is for demo purposes only.
    It does not tested in production environment.
    """

    checker_cls = MasterFailoverThread

    def __init__(self, *args, **kwargs):
        super(MasterFailoverReplicationRouter, self).__init__(*args, **kwargs)
        self.MASTER = self.DEFAULT_DB_ALIAS

    @property
    def master(self):
        return self.MASTER

    @master.setter
    def master(self, alias):

        if self.MASTER == alias:
            # This should not happen. Just in case
            logger.info("'%s' is already a master. Skip.", alias)
            return

        with self.rlock:
            was_master = self.MASTER
            self.SLAVES = list(set(self.SLAVES + [was_master,]))
            self.MASTER = alias
            self.SLAVES.remove(alias)
            logger.info("Change master from '%s' to '%s'. Slaves now: %s", was_master, alias, self.SLAVES)
