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
import os
import sys

from tests.helper import TestHelper
from tests.zope import has_zope, has_testresources

from storm.locals import create_database, Store, Unicode, Int
from storm.exceptions import IntegrityError

if has_zope and has_testresources:
    from storm.zope.schema import ZSchema
    from storm.zope.testing import ZStormResourceManager


PATCH = """
def apply(store):
    store.execute('ALTER TABLE test ADD COLUMN bar INT')
"""


class ZStormResourceManagerTest(TestHelper):

    def is_supported(self):
        return has_zope and has_testresources

    def setUp(self):
        super(ZStormResourceManagerTest, self).setUp()
        self._package_dir = self.makeDir()
        sys.path.append(self._package_dir)
        patch_dir = os.path.join(self._package_dir, "patch_package")
        os.mkdir(patch_dir)
        self.makeFile(path=os.path.join(patch_dir, "__init__.py"), content="")
        self.makeFile(path=os.path.join(patch_dir, "patch_1.py"),
                      content=PATCH)
        import patch_package
        create = ["CREATE TABLE test (foo TEXT UNIQUE, bar INT)"]
        drop = ["DROP TABLE test"]
        delete = ["DELETE FROM test"]
        schema = ZSchema(create, drop, delete, patch_package)
        uri = "sqlite:///%s" % self.makeFile()
        self.resource = ZStormResourceManager({"test": (uri, schema)})
        self.store = Store(create_database(uri))

    def tearDown(self):
        del sys.modules["patch_package"]
        sys.path.remove(self._package_dir)
        if "patch_1" in sys.modules:
            del sys.modules["patch_1"]
        super(ZStormResourceManagerTest, self).tearDown()

    def test_make(self):
        """
        L{ZStormResourceManager.make} returns a L{ZStorm} resource that can be
        used to get the registered L{Store}s.
        """
        zstorm = self.resource.make([])
        store = zstorm.get("test")
        self.assertEqual([], list(store.execute("SELECT foo, bar FROM test")))

    def test_make_upgrade(self):
        """
        L{ZStormResourceManager.make} upgrades the schema if needed.
        """
        self.store.execute("CREATE TABLE patch "
                           "(version INTEGER NOT NULL PRIMARY KEY)")
        self.store.execute("CREATE TABLE test (foo TEXT)")
        self.store.commit()
        zstorm = self.resource.make([])
        store = zstorm.get("test")
        self.assertEqual([], list(store.execute("SELECT bar FROM test")))

    def test_make_delete(self):
        """
        L{ZStormResourceManager.make} deletes the data from all tables to make
        sure that tests run against a clean database.
        """
        self.store.execute("CREATE TABLE patch "
                           "(version INTEGER NOT NULL PRIMARY KEY)")
        self.store.execute("CREATE TABLE test (foo TEXT)")
        self.store.execute("INSERT INTO test (foo) VALUES ('data')")
        self.store.commit()
        zstorm = self.resource.make([])
        store = zstorm.get("test")
        self.assertEqual([], list(store.execute("SELECT foo FROM test")))

    def test_clean_flush(self):
        """
        L{ZStormResourceManager.clean} tries to flush the stores to make sure
        that they are all in a consistent state.
        """

        class Test(object):
            __storm_table__ = "test"
            foo = Unicode()
            bar = Int(primary=True)

            def __init__(self, foo, bar):
                self.foo = foo
                self.bar = bar

        zstorm = self.resource.make([])
        store = zstorm.get("test")
        store.add(Test(u"data", 1))
        store.add(Test(u"data", 2))
        self.assertRaises(IntegrityError, self.resource.clean, zstorm)

    def test_clean_delete(self):
        """
        L{ZStormResourceManager.clean} cleans the database tables from the data
        created by the tests.
        """
        zstorm = self.resource.make([])
        store = zstorm.get("test")
        store.execute("INSERT INTO test (foo, bar) VALUES ('data', 123)")
        store.commit()
        self.resource.clean(zstorm)
        self.assertEqual([], list(self.store.execute("SELECT * FROM test")))
