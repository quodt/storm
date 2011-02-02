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
import threading
import weakref
import gc

from tests.helper import TestHelper
from tests.zope import has_transaction, has_zope_component

if has_transaction:
    import transaction
    from storm.zope.interfaces import IZStorm, ZStormError
    from storm.zope.zstorm import ZStorm, StoreDataManager

if has_zope_component:
    from zope.component import provideUtility, getUtility

from storm.exceptions import OperationalError
from storm.locals import Store


class ZStormTest(TestHelper):

    def is_supported(self):
        return has_transaction

    def setUp(self):
        self.zstorm = ZStorm()

    def tearDown(self):
        # Reset the utility to cleanup the StoreSynchronizer's from the
        # transaction.
        self.zstorm._reset()
        # Free the transaction to avoid having errors that cross
        # test cases.
        transaction.manager.free(transaction.get())

    def test_create(self):
        store = self.zstorm.create(None, "sqlite:")
        self.assertTrue(isinstance(store, Store))

    def test_create_twice_unnamed(self):
        store = self.zstorm.create(None, "sqlite:")
        store.execute("CREATE TABLE test (id INTEGER)")
        store.commit()

        store = self.zstorm.create(None, "sqlite:")
        self.assertRaises(OperationalError,
                          store.execute, "SELECT * FROM test")

    def test_create_twice_same_name(self):
        store = self.zstorm.create("name", "sqlite:")
        self.assertRaises(ZStormError, self.zstorm.create, "name", "sqlite:")

    def test_create_and_get_named(self):
        store = self.zstorm.create("name", "sqlite:")
        self.assertTrue(self.zstorm.get("name") is store)

    def test_create_and_get_named_another_thread(self):
        store = self.zstorm.create("name", "sqlite:")

        raised = []

        def f():
            try:
                self.zstorm.get("name")
            except ZStormError:
                raised.append(True)
        thread = threading.Thread(target=f)
        thread.start()
        thread.join()

        self.assertTrue(raised)

    def test_get_unexistent(self):
        self.assertRaises(ZStormError, self.zstorm.get, "name")

    def test_get_with_uri(self):
        store = self.zstorm.get("name", "sqlite:")
        self.assertTrue(isinstance(store, Store))
        self.assertTrue(self.zstorm.get("name") is store)
        self.assertTrue(self.zstorm.get("name", "sqlite:") is store)

    def test_set_default_uri(self):
        self.zstorm.set_default_uri("name", "sqlite:")
        store = self.zstorm.get("name")
        self.assertTrue(isinstance(store, Store))

    def test_create_default(self):
        self.zstorm.set_default_uri("name", "sqlite:")
        store = self.zstorm.create("name")
        self.assertTrue(isinstance(store, Store))

    def test_create_default_twice(self):
        self.zstorm.set_default_uri("name", "sqlite:")
        self.zstorm.create("name")
        self.assertRaises(ZStormError, self.zstorm.create, "name")

    def test_iterstores(self):
        store1 = self.zstorm.create(None, "sqlite:")
        store2 = self.zstorm.create(None, "sqlite:")
        store3 = self.zstorm.create("name", "sqlite:")
        stores = []
        for name, store in self.zstorm.iterstores():
            stores.append((name, store))
        self.assertEquals(len(stores), 3)
        self.assertEquals(set(stores),
                          set([(None, store1), (None, store2),
                               ("name", store3)]))

    def test_get_name(self):
        store = self.zstorm.create("name", "sqlite:")
        self.assertEquals(self.zstorm.get_name(store), "name")

    def test_get_name_with_removed_store(self):
        store = self.zstorm.create("name", "sqlite:")
        self.assertEquals(self.zstorm.get_name(store), "name")
        self.zstorm.remove(store)
        self.assertEquals(self.zstorm.get_name(store), None)

    def test_default_databases(self):
        self.zstorm.set_default_uri("name1", "sqlite:1")
        self.zstorm.set_default_uri("name2", "sqlite:2")
        self.zstorm.set_default_uri("name3", "sqlite:3")
        default_uris = self.zstorm.get_default_uris()
        self.assertEquals(default_uris, {"name1": "sqlite:1",
                                         "name2": "sqlite:2",
                                         "name3": "sqlite:3"})

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

    def assertNotInTransaction(self, store):
        """Check that the given store is not joined to the transaction."""
        self.assertTrue(not self._isInTransaction(store),
                        "%r should not be joined to the transaction" % store)

    def test_wb_store_joins_transaction_on_register_event(self):
        """The Store joins the transaction when register-transaction
        is emitted.

        The Store tests check the various operations that trigger this
        event.
        """
        store = self.zstorm.get("name", "sqlite:")
        self.assertNotInTransaction(store)
        store._event.emit("register-transaction")
        self.assertInTransaction(store)

    def test_wb_store_joins_transaction_on_use_after_commit(self):
        store = self.zstorm.get("name", "sqlite:")
        store.execute("SELECT 1")
        transaction.commit()
        self.assertNotInTransaction(store)
        store.execute("SELECT 1")
        self.assertInTransaction(store)

    def test_wb_store_joins_transaction_on_use_after_abort(self):
        store = self.zstorm.get("name", "sqlite:")
        store.execute("SELECT 1")
        transaction.abort()
        self.assertNotInTransaction(store)
        store.execute("SELECT 1")
        self.assertInTransaction(store)

    def test_remove(self):
        removed_store = self.zstorm.get("name", "sqlite:")
        self.zstorm.remove(removed_store)
        for name, store in self.zstorm.iterstores():
            self.assertNotEquals(store, removed_store)
        self.assertRaises(ZStormError, self.zstorm.get, "name")

    def test_wb_removed_store_does_not_join_transaction(self):
        """If a store has been removed, it will not join the transaction."""
        store = self.zstorm.get("name", "sqlite:")
        self.zstorm.remove(store)
        store.execute("SELECT 1")
        self.assertNotInTransaction(store)

    def test_wb_removed_store_does_not_join_future_transactions(self):
        """If a store has been removed after joining a transaction, it
        will not join new transactions."""
        store = self.zstorm.get("name", "sqlite:")
        store.execute("SELECT 1")
        self.zstorm.remove(store)
        self.assertInTransaction(store)

        transaction.abort()
        store.execute("SELECT 1")
        self.assertNotInTransaction(store)

    def test_wb_cross_thread_store_does_not_join_transaction(self):
        """If a zstorm registered thread crosses over to another thread,
        it will not be usable."""
        store = self.zstorm.get("name", "sqlite:")

        failures = []
        def f():
            # We perform this twice to show that ZStormError is raised
            # consistently (i.e. not just the first time).
            for i in range(2):
                try:
                    store.execute("SELECT 1")
                except ZStormError:
                    failures.append("ZStormError raised")
                except Exception, exc:
                    failures.append("Expected ZStormError, got %r" % exc)
                else:
                    failures.append("Expected ZStormError, nothing raised")
                if self._isInTransaction(store):
                    failures.append("store was joined to transaction")
        thread = threading.Thread(target=f)
        thread.start()
        thread.join()
        self.assertEqual(failures, ["ZStormError raised"] * 2)

    def test_wb_reset(self):
        """_reset is used to reset the zstorm utility between zope test runs.
        """
        store = self.zstorm.get("name", "sqlite:")
        self.zstorm._reset()
        self.assertEqual(list(self.zstorm.iterstores()), [])

    def test_store_strong_reference(self):
        """
        The zstorm utility should be a strong reference to named stores so that
        it doesn't recreate stores uselessly.
        """
        store = self.zstorm.get("name", "sqlite:")
        store_ref = weakref.ref(store)
        transaction.abort()
        del store
        gc.collect()
        self.assertNotIdentical(store_ref(), None)
        store = self.zstorm.get("name")
        self.assertIdentical(store_ref(), store)


class ZStormUtilityTest(TestHelper):

    def is_supported(self):
        return has_zope_component

    def test_utility(self):
        provideUtility(ZStorm())
        self.assertTrue(isinstance(getUtility(IZStorm), ZStorm))

