#!/bin/env python
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

############################################################################
#
#   @file test_sys_bitmap_sc.py
#   @date 2020-02-20
#
#############################################################################

"""
测试bitmap data type
"""
import sys
import time

sys.path.append("../")
from lib import palo_config
from lib import palo_client
from lib import util
from lib import palo_job
import palo_logger
import palo_exception


client = None

#日志 异常 对象
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage
PaloClientException = palo_exception.PaloException

config = palo_config.config
compare = 'test_query_qa.test'


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port)
    client.init()


def wait_end(database_name):
    """
    wait to finished
    """
    ret = True
    print('waitint for load...')
    state = None
    while ret:
        job_list = client.get_load_job_list(database_name=database_name)
        state = palo_job.LoadJob(job_list[-1]).get_state()
        # print(state)
        if state == "FINISHED" or state == "CANCELLED":
            print(state)
            ret = False
        time.sleep(1)
    assert state == "FINISHED"


def execute(line):
    """execte sql"""
    print(line)
    palo_result = client.execute(line)
    print(palo_result)
    return palo_result


def init(db_name, table_name):
    """
    create db, table, bulk load, batch load
    Args:
        db_name:
        table_name:
        create_sql:
        key_column:

    Returns:
    """
    line = 'DROP DATABASE IF EXISTS %s' % db_name
    client.execute(line)
    client.create_database(db_name)
    client.use(db_name)
    # client.execute('drop table if exists %s' % table_name)
    line = 'CREATE TABLE %s (\
    `id`  int COMMENT "", \
    `id1` tinyint COMMENT "", \
    `c_float` float SUM COMMENT "", \
    `bitmap_set` bitmap bitmap_union COMMENT "" \
    ) ENGINE=OLAP \
     DISTRIBUTED BY HASH(`id`, `id1`) BUCKETS 5 \
     PROPERTIES ( \
    "storage_type" = "COLUMN" \
    );' % table_name
    execute(line)
    line = 'insert into %s select k4, k1, k9, bitmap_hash(k9) from %s' % (table_name, compare)
    execute(line)
    wait_end(db_name)


def test_sc_add_bitmap_column():
    """
    {
    "title": "test_sys_bitmap_sc.test_sc_add_bitmap_column",
    "describe": "schema change, add column and drop",
    "tag": "function,p1"
    }
    """
    """schema change, add column and drop"""
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    init(db_name, table_name)
    # add column
    column = ['bitmap_add bitmap bitmap_union', 'bitmap_add_1 bitmap bitmap_union']
    ret = client.schema_change(table_name, add_column_list=column, is_wait=True)
    assert ret, 'add bitmap column failed'
    line = 'select bitmap_union_count(bitmap_set), bitmap_union_count(bitmap_add),' \
           ' bitmap_union_count(bitmap_add_1) from %s' % table_name
    ret1 = client.execute(line)
    # 有两对计算hash值时冲突了
    assert ret1 == ((59954, 0, 0),)
    # drop column
    column = ['bitmap_add_1']
    ret = client.schema_change(table_name, drop_column_list=column, is_wait=True)
    assert ret, 'drop bitmap column failed'
    line = 'select bitmap_union_count(bitmap_add), bitmap_union_count(bitmap_add) from %s' % table_name
    ret2 = client.execute(line)
    assert ret1[0][2] == ret2[0][1]
    assert ret1[0][1] == ret2[0][0]
    client.clean(db_name)


def test_sc_drop_bitmap_column():
    """
    {
    "title": "test_sys_bitmap_sc.test_sc_drop_bitmap_column",
    "describe": "schema change, drop column",
    "tag": "function,p1"
    }
    """
    """schema change, drop column"""
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    init(db_name, table_name)
    column = ['bitmap_set']
    ret = client.schema_change(table_name, drop_column_list=column, is_wait=True)
    assert ret, 'drop bitmap column failed'
    line = 'select bitmap_union_count(bitmap_set) from %s' % table_name
    flag = 0
    try:
        print(line)
        ret = client.execute(line)
        flag = 1
    except Exception as e:
        print(str(e))
    assert flag == 0, 'expect select execute error'
    line = 'insert into %s select k4, k1, k9 from %s' % (table_name, compare)
    execute(line)
    wait_end(db_name)
    line = 'select * from %s order by 1, 2, 3 limit 100' % table_name
    print(line)
    try:
        ret = client.execute(line)
    except Exception as e:
        print(str(e))
        assert 0 == 1
    client.clean(db_name)


def test_sc_modified_bitmap_column():
    """
    {
    "title": "test_sys_bitmap_sc.test_sc_modified_bitmap_column",
    "describe": "schema change, 将bitmap列变为其他类型/key",
    "tag": "function,p1,fuzz"
    }
    """
    """schema change, 将bitmap列变为其他类型/key"""
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    init(db_name, table_name) 
    client.use(db_name)
    # modify bitmap column, expect false
    column_list = ['bitmap_set bitmap default "0"', 'bitmap_set int replace default "0" after id1',
                   'bitmap_set int replace default "0"', 'bitmap_set varchar(256) replace default ""',
                   'bitmap_set varchar(256)  default "" after id1', 
                   'bitmap_set char(256) replace default ""',
                   'bitmap_set bigint replace default ""', 'bitmap_set double replace default "0.01"',
                   'bitmap_set datetime replace default ""']
    for column in column_list:
        modify_column = [column, ]
        flag = 1
        try:
            ret = client.schema_change(table_name, modify_column_list=modify_column, is_wait=True)
            flag = 0
        except Exception as e:
            print(str(e))
        assert flag == 1

    order_list = ['id1', 'id', 'c_float', 'bitmap_set']
    ret = client.schema_change(table_name, order_column_list=order_list, is_wait=True)
    assert ret
    # check order
    ret = client.desc_table(table_name)
    assert ret[3][0] == 'bitmap_set'
    client.clean(db_name)


def test_sc_rollup_bitmap_column():
    """
    {
    "title": "test_sys_bitmap_sc.test_sc_rollup_bitmap_column",
    "describe": "schema change, create rollup and delete rollup",
    "tag": "function,p1"
    }
    """
    """schema change, create rollup and delete rollup"""
    db_name, table_name, index_name = util.gen_name_list()
    init(db_name, table_name)
    line1 = 'select id1, id, bitmap_union_count(bitmap_set) from %s group by id1, id order by 1, 2' \
            % table_name
    ret1_1 = client.execute(line1)
    line2 = 'select id1, bitmap_union_count(bitmap_set) from %s group by id, id1 order by 1, 2' % table_name
    ret2_1 = client.execute(line2)
    index_name_derive = index_name + "_1"
    rollup_column = ['id1', 'id', 'bitmap_set']
    r = client.create_rollup_table(table_name, index_name, rollup_column, is_wait=True)
    assert r
    ret1_2 = client.execute(line1)
    # ret2_2 = client.execute(line2)
    util.check(ret1_1, ret1_2)

    rollup_column_1 = ['id1', 'bitmap_set']
    r = client.create_rollup_table(table_name, index_name_derive, rollup_column_1, 
                                   base_index_name=index_name, is_wait=True)
    assert r
    ret3_2 = client.execute(line2)
    util.check(ret2_1, ret3_2)
    try:
        client.drop_rollup_table(table_name, index_name)
    except Exception as e:
        print(str(e))
        assert 0 == 1
    client.clean(db_name)


if __name__ == '__main__':
    setup_module()
    # test_sc_add_bitmap_column()
    # test_sc_drop_bitmap_column()
    # test_sc_modified_bitmap_column()
    test_sc_rollup_bitmap_column()

