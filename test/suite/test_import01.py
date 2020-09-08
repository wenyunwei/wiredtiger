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
# test_import01.py
#

import os, shutil
import wiredtiger, wttest
from wtdataset import SimpleDataSet

def timestamp_str(t):
    return '%x' % t

class test_import01(wttest.WiredTigerTestCase):
    conn_config = ('cache_size=50MB,log=(enabled),statistics=(all)')
    session_config = 'isolation=snapshot'

    def test_import(self):
        file_object = 'test_import_file'
        uri = 'file:' + file_object

        # mongodb create params
        create_params_mdb_4k = ('access_pattern_hint=none,allocation_size=4K,app_metadata=,assert=(commit_timestamp=none,durable_timestamp=none,read_timestamp=none),block_allocation=best,block_compressor="zlib",cache_resident=false,checksum="uncompressed",colgroups=,collator=,columns=,dictionary=0,encryption=(keyid=,name=),exclusive=false,extractor=,format=btree,huffman_key=,huffman_value=,ignore_in_memory_cache_size=false,immutable=false,internal_item_max=0,internal_key_max=1607,internal_key_truncate=true,internal_page_max=65536,key_format=u,key_gap=14,leaf_item_max=0,leaf_key_max=98,leaf_page_max=4096,leaf_value_max=40960,log=(enabled=true),memory_page_image_max=0,memory_page_max=4194304,os_cache_dirty_max=0,os_cache_max=0,prefix_compression=false,prefix_compression_min=4,source=,split_deepen_min_child=0,split_deepen_per_child=0,split_pct=86,type=file,value_format=u')
        # mongodb create params with allocation_size=512B
        create_params_mdb_512 = ('access_pattern_hint=none,allocation_size=512B,app_metadata=,assert=(commit_timestamp=none,durable_timestamp=none,read_timestamp=none),block_allocation=best,block_compressor="zlib",cache_resident=false,checksum="uncompressed",colgroups=,collator=,columns=,dictionary=0,encryption=(keyid=,name=),exclusive=false,extractor=,format=btree,huffman_key=,huffman_value=,ignore_in_memory_cache_size=false,immutable=false,internal_item_max=0,internal_key_max=1607,internal_key_truncate=true,internal_page_max=65536,key_format=u,key_gap=14,leaf_item_max=0,leaf_key_max=98,leaf_page_max=4096,leaf_value_max=40960,log=(enabled=true),memory_page_image_max=0,memory_page_max=4194304,os_cache_dirty_max=0,os_cache_max=0,prefix_compression=false,prefix_compression_min=4,source=,split_deepen_min_child=0,split_deepen_per_child=0,split_pct=86,type=file,value_format=u')

        create_params = create_params_mdb_4k
        self.session.create(uri, create_params)

        c = self.session.open_cursor(uri)
        self.session.begin_transaction()
        c[b'13'] = b'\x01\x02xxx\x03\x04'
        self.session.commit_transaction()
        c.close()

        # Perform a checkpoint.
        self.session.checkpoint()

        c = self.session.open_cursor(uri)
        self.session.begin_transaction()
        c[b'13'] = b'\x01\x02xxx\x03\x04'
        self.session.commit_transaction()
        c.close()

        # Perform a checkpoint.
        self.session.checkpoint()

        # Close the connection.
        self.close_conn()

        # Create a new database and connect to it.
        newdir = 'IMPORT_DB'
        shutil.rmtree(newdir, ignore_errors=True)
        os.mkdir(newdir)
        self.conn = self.setUpConnectionOpen(newdir)
        self.session = self.setUpSessionOpen(self.conn)
        self.session.create('table:ximportx', create_params)

        # Copy over the datafiles for the object we want to import.
        fullname = os.path.join('.', file_object)
        if os.path.isfile(fullname) and "WiredTiger.lock" not in fullname and \
            "Tmplog" not in fullname and "Preplog" not in fullname:
            shutil.copy(fullname, newdir)

        self.session.live_import(uri)

        # Verify object.
        self.session.verify(uri)

        # Open cursor.
        c = self.session.open_cursor(uri)
        self.session.begin_transaction()
        c[b'100'] = b'\x01\x02zzz\x03\x04'
        self.session.commit_transaction()
        c.close()

        # Perform a checkpoint.
        self.session.checkpoint()

if __name__ == '__main__':
    wttest.run()
