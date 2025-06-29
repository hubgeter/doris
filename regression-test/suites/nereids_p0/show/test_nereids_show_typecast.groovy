// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_nereids_show_typecast") {
    sql """DROP DATABASE IF EXISTS test_show_typecast_db;"""
    sql """CREATE DATABASE IF NOT EXISTS test_show_typecast_db;"""
    sql """DROP TABLE IF EXISTS test_show_typecast_db.test_show_type_cast_tbl"""

    checkNereidsExecute("show type_cast from test_show_typecast_db;")
    checkNereidsExecute("show type_cast in test_show_typecast_db;")

    def res1 = sql """show type_cast from test_show_typecast_db"""
    assertEquals(true, res1.size() > 0)

}
