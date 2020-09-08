#!/usr/bin/env python
#
# Public Domain 2014-2020 MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# test_import03.py
# Import a column store into a running database.

import os, shutil
import wiredtiger, wttest

def timestamp_str(t):
    return '%x' % t

class test_import03(wttest.WiredTigerTestCase):
    conn_config = ('cache_size=50MB,log=(enabled),statistics=(all)')
    session_config = 'isolation=snapshot'
    nentries = 1000

    def copy_file(self, file_name, old_dir, new_dir):
        if os.path.isfile(file_name) and "WiredTiger.lock" not in file_name and \
            "Tmplog" not in file_name and "Preplog" not in file_name:
            shutil.copy(os.path.join(old_dir, file_name), new_dir)

    def populate(self, uri):
        cursor = self.session.open_cursor(uri, None, None)
        for i in range(0, self.nentries):
            square = i * i
            cube = square * i
            cursor[(i, 'key' + str(i))] = \
                ('val' + str(square), square, 'val' + str(cube), cube)
        cursor.close()

    def check_entries(self, uri):
        cursor = self.session.open_cursor(uri, None, None)
        # spot check via search
        n = self.nentries
        for i in (n // 5, 0, n - 1, n - 2, 1):
            cursor.set_key(i, 'key' + str(i))
            square = i * i
            cube = square * i
            cursor.search()
            (s1, i2, s3, i4) = cursor.get_values()
            self.assertEqual(s1, 'val' + str(square))
            self.assertEqual(i2, square)
            self.assertEqual(s3, 'val' + str(cube))
            self.assertEqual(i4, cube)

        i = 0
        count = 0
        # then check all via cursor
        cursor.reset()
        for ikey, skey, s1, i2, s3, i4 in cursor:
            i = ikey
            square = i * i
            cube = square * i
            self.assertEqual(ikey, i)
            self.assertEqual(skey, 'key' + str(i))
            self.assertEqual(s1, 'val' + str(square))
            self.assertEqual(i2, square)
            self.assertEqual(s3, 'val' + str(cube))
            self.assertEqual(i4, cube)
            count += 1
        cursor.close()
        self.assertEqual(count, n)

    def create_colgroups(self, table_name):
        self.session.create('table:' + table_name, 'key_format=iS,value_format=SiSi,'
                            'columns=(ikey,Skey,S1,i2,S3,i4),colgroups=(c1,c2)')
        self.session.create('colgroup:' + table_name + ':c1',
                            'allocation_size=512,columns=(S1,i2)')
        self.session.create('colgroup:' + table_name + ':c2',
                            'allocation_size=512,columns=(S3,i4)')
        self.populate('table:' + table_name)
        self.check_entries('table:' + table_name)

    def test_colgroup_import(self):
        original_db_table = 'original_db_table'
        uri = 'table:' + original_db_table
        
        # Create table with multiple columns.
        self.create_colgroups(original_db_table)

        # Perform a checkpoint.
        self.session.checkpoint()

        # Export the metadata for the table and colgroups.
        c = self.session.open_cursor('metadata:', None, None)
        original_db_table_config = c[uri]
        original_db_c1_config = c['colgroup:original_db_table:c1']
        original_db_c2_config = c['colgroup:original_db_table:c2']
        c.close()

        # Close the connection.
        self.close_conn()

        # Create a new database and connect to it.
        import_db_create_config = ('allocation_size=4K,key_format=i,value_format=S')
        newdir = 'IMPORT_DB'
        shutil.rmtree(newdir, ignore_errors=True)
        os.mkdir(newdir)
        self.conn = self.setUpConnectionOpen(newdir)
        self.session = self.setUpSessionOpen(self.conn)
        self.session.create('table:some_db_table', import_db_create_config)

        # Copy over the datafiles for the object we want to import.
        self.copy_file(original_db_table + '.wt', '.', newdir)
        self.copy_file(original_db_table + '_c1.wt', '.', newdir)
        self.copy_file(original_db_table + '_c2.wt', '.', newdir)

        # Do the import.
        self.session.live_import(uri, original_db_table_config)
        self.session.live_import('colgroup:' + original_db_table + ':c1', original_db_c1_config)
        self.session.live_import('colgroup:' + original_db_table + ':c2', original_db_c2_config)

        # Verify object.
        self.check_entries(uri)
        self.session.verify(uri)

        # Compare metadata.
        c = self.session.open_cursor('metadata:', None, None)
        import_db_table_config = c[uri]
        import_db_c1_config = c['colgroup:original_db_table:c1']
        import_db_c2_config = c['colgroup:original_db_table:c2']
        c.close()

        self.assertEqual(original_db_table_config, import_db_table_config)
        self.assertEqual(original_db_c1_config, import_db_c1_config)
        self.assertEqual(original_db_c2_config, import_db_c2_config)

if __name__ == '__main__':
    wttest.run()
