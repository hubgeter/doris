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
suite("q49_rewrite_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set spill_min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_ck_p1;
  """
  qt_q49_rewrite """
(SELECT
  'web' channel
, web.item
, web.return_ratio
, web.return_rank
, web.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        ws.ws_item_sk item
      , (CAST(sum(COALESCE(wr.wr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(wr.wr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        web_sales ws
      LEFT JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number)
         AND (ws.ws_item_sk = wr.wr_item_sk)
      , date_dim
      WHERE (wr.wr_return_amt > 10000)
         AND (ws.ws_net_profit > 1)
         AND (ws.ws_net_paid > 0)
         AND (ws.ws_quantity > 0)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2001)
         AND (d_moy = 12)
      GROUP BY ws.ws_item_sk
   )  in_web
)  web
WHERE (web.return_rank <= 10)
   OR (web.currency_rank <= 10))
UNION (SELECT
  'catalog' channel
, catalog.item
, catalog.return_ratio
, catalog.return_rank
, catalog.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        cs.cs_item_sk item
      , (CAST(sum(COALESCE(cr.cr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(cr.cr_return_amount, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        catalog_sales cs
      LEFT JOIN catalog_returns cr ON (cs.cs_order_number = cr.cr_order_number)
         AND (cs.cs_item_sk = cr.cr_item_sk)
      , date_dim
      WHERE (cr.cr_return_amount > 10000)
         AND (cs.cs_net_profit > 1)
         AND (cs.cs_net_paid > 0)
         AND (cs.cs_quantity > 0)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2001)
         AND (d_moy = 12)
      GROUP BY cs.cs_item_sk
   )  in_cat
) catalog 
WHERE (catalog.return_rank <= 10)
   OR (catalog.currency_rank <= 10))
UNION (SELECT
  'store' channel
, store.item
, store.return_ratio
, store.return_rank
, store.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        sts.ss_item_sk item
      , (CAST(sum(COALESCE(sr.sr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(sr.sr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        store_sales sts
      LEFT JOIN store_returns sr ON (sts.ss_ticket_number = sr.sr_ticket_number)
         AND (sts.ss_item_sk = sr.sr_item_sk)
      , date_dim
      WHERE (sr.sr_return_amt > 10000)
         AND (sts.ss_net_profit > 1)
         AND (sts.ss_net_paid > 0)
         AND (sts.ss_quantity > 0)
         AND (ss_sold_date_sk = d_date_sk)
         AND (d_year = 2001)
         AND (d_moy = 12)
      GROUP BY sts.ss_item_sk
   )  in_store
)  store
WHERE (store.return_rank <= 10)
   OR (store.currency_rank <= 10))
ORDER BY 1 ASC, 4 ASC, 5 ASC, 2 ASC
LIMIT 100
"""
}
