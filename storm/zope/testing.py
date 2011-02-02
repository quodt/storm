#
# Copyright (c) 2006, 2007 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# Storm is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; either version 2.1 of
# the License, or (at your option) any later version.
#
# Storm is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
import transaction
from testresources import TestResourceManager
from zope.component import provideUtility

from storm.zope.zstorm import ZStorm


class ZStormResourceManager(TestResourceManager):
    """Provide a L{ZStorm} resource to be used in test cases.

    The constructor is passed the details of the L{Store}s to be registered
    in the provided L{ZStore} resource. Then the C{make} and C{clean} methods
    make sure that such L{Store}s are properly setup and cleaned for each test.

    @param databases: A C{dict} with the form C{name: (uri, schema)}, where
        'name' is the name of the store to be registered in the L{ZStorm}
        resource, 'uri' is the database URI needed to create the store and
        'schema' is the L{Schema} for the tables in the store.
    """

    def __init__(self, databases):
        super(ZStormResourceManager, self).__init__()
        self._databases = databases
        self._zstorm = None
        self._commits = {}

    def make(self, dependencies):
        """Create a L{ZStorm} resource to be used by tests.

        @return: A L{ZStorm} object that will be shared among all tests using
            this resource manager.
        """
        if self._zstorm is None:
            zstorm = ZStorm()
            provideUtility(zstorm)
            for name, (uri, schema) in self._databases.iteritems():
                zstorm.set_default_uri(name, uri)
                store = zstorm.get(name)
                self._set_commit_proxy(store)
                schema.upgrade(store)
                # Clean up tables here to ensure that the first test run starts
                # with an empty db
                schema.delete(store)
            self._zstorm = zstorm
        return self._zstorm

    def _set_commit_proxy(self, store):
        """Set a commit proxy to keep track of commits and clean up the tables.

        @param store: The L{Store} to set the commit proxy on. Any commit on
            this store will result in the associated tables to be cleaned upon
            tear down.
        """
        store.__real_commit__ = store.commit

        def commit_proxy():
            self._commits[store] = True
            store.__real_commit__()

        store.commit = commit_proxy

    def clean(self, resource):
        """Clean up the stores after a test."""
        try:
            for name, store in self._zstorm.iterstores():
                # Ensure that the store is in a consistent state
                store.flush()
        finally:
            transaction.abort()

        # Clean up tables after each test if a commit was made
        for name, store in self._zstorm.iterstores():
            if store in self._commits:
                _, schema = self._databases[name]
                schema.delete(store)
        self._commits = {}
