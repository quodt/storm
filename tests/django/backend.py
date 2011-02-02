#
# Copyright (c) 2008 Canonical
#
# Written by James Henstridge <jamesh@canonical.com>
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

import os

try:
    import django
    import transaction
except ImportError:
    have_django_and_transaction = False
else:
    have_django_and_transaction = True
    from django.conf import settings
    from storm.django import stores
    from storm.zope.zstorm import global_zstorm, StoreDataManager

from tests.helper import TestHelper


class DjangoBackendTests(object):

    def is_supported(self):
        return have_django_and_transaction and self.get_store_uri() is not None

    def setUp(self):
        super(DjangoBackendTests, self).setUp()
        settings.configure(STORM_STORES={})
        settings.MIDDLEWARE_CLASSES += (
            "storm.django.middleware.ZopeTransactionMiddleware",)

        settings.DATABASE_ENGINE = "storm.django.backend"
        settings.DATABASE_NAME = "django"
        settings.STORM_STORES["django"] = self.get_store_uri()
        stores.have_configured_stores = False
        self.create_tables()

    def tearDown(self):
        transaction.abort()
        self.drop_tables()
        if django.VERSION >= (1, 1):
            settings._wrapped = None
        else:
            settings._target = None
        global_zstorm._reset()
        stores.have_configured_stores = False
        transaction.manager.free(transaction.get())
        super(DjangoBackendTests, self).tearDown()

    def get_store_uri(self):
        raise NotImplementedError

    def get_wrapper_class(self):
        raise NotImplementedError

    def create_tables(self):
        raise NotImplementedError

    def drop_tables(self):
        raise NotImplementedError

    def make_wrapper(self):
        from storm.django.backend import base
        if django.VERSION >= (1, 1):
            wrapper = base.DatabaseWrapper({
                    'DATABASE_HOST': settings.DATABASE_HOST,
                    'DATABASE_NAME': settings.DATABASE_NAME,
                    'DATABASE_OPTIONS': settings.DATABASE_OPTIONS,
                    'DATABASE_PASSWORD': settings.DATABASE_PASSWORD,
                    'DATABASE_PORT': settings.DATABASE_PORT,
                    'DATABASE_USER': settings.DATABASE_USER,
                    'TIME_ZONE': settings.TIME_ZONE,
                    })
        else:
            wrapper = base.DatabaseWrapper(**settings.DATABASE_OPTIONS)
        return wrapper

    def test_create_wrapper(self):
        wrapper = self.make_wrapper()
        self.assertTrue(isinstance(wrapper, self.get_wrapper_class()))

        # The wrapper uses the same database connection as the store.
        store = stores.get_store("django")
        self.assertEqual(store._connection._raw_connection, wrapper.connection)

    def _isInTransaction(self, store):
        """Check if a Store is part of the current transaction."""
        for dm in transaction.get()._resources:
            if isinstance(dm, StoreDataManager) and dm._store is store:
                return True
        return False

    def assertInTransaction(self, store):
        """Check that the given store is joined to the transaction."""
        self.assertTrue(self._isInTransaction(store),
                        "%r should be joined to the transaction" % store)

    def test_using_wrapper_joins_transaction(self):
        wrapper = self.make_wrapper()
        cursor = wrapper.cursor()
        cursor.execute("SELECT 1")
        self.assertInTransaction(stores.get_store("django"))

    def test_commit(self):
        wrapper = self.make_wrapper()
        cursor = wrapper.cursor()
        cursor.execute("INSERT INTO django_test (title) VALUES ('foo')")
        wrapper._commit()

        cursor = wrapper.cursor()
        cursor.execute("SELECT title FROM django_test")
        result = cursor.fetchall()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], "foo")

    def test_rollback(self):
        wrapper = self.make_wrapper()
        cursor = wrapper.cursor()
        cursor.execute("INSERT INTO django_test (title) VALUES ('foo')")
        wrapper._rollback()

        cursor = wrapper.cursor()
        cursor.execute("SELECT title FROM django_test")
        result = cursor.fetchall()
        self.assertEqual(len(result), 0)


class PostgresDjangoBackendTests(DjangoBackendTests, TestHelper):

    def get_store_uri(self):
        return os.environ.get("STORM_POSTGRES_URI")

    def get_wrapper_class(self):
        from storm.django.backend import base
        return base.PostgresStormDatabaseWrapper

    def create_tables(self):
        store = stores.get_store("django")
        store.execute("CREATE TABLE django_test ("
                      "  id SERIAL PRIMARY KEY,"
                      "  title TEXT)")
        transaction.commit()

    def drop_tables(self):
        store = stores.get_store("django")
        store.execute("DROP TABLE django_test")
        transaction.commit()


class MySQLDjangoBackendTests(DjangoBackendTests, TestHelper):

    def get_store_uri(self):
        return os.environ.get("STORM_MYSQL_URI")

    def get_wrapper_class(self):
        from storm.django.backend import base
        return base.MySQLStormDatabaseWrapper

    def create_tables(self):
        store = stores.get_store("django")
        store.execute("CREATE TABLE django_test ("
                      "  id INT AUTO_INCREMENT PRIMARY KEY,"
                      "  title TEXT) ENGINE=InnoDB")
        transaction.commit()

    def drop_tables(self):
        store = stores.get_store("django")
        store.execute("DROP TABLE django_test")
        transaction.commit()
