/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/group_by_vec_physical_operator.h"
#include "common/log/log.h"
#include "sql/expr/expression_tuple.h"
#include "sql/expr/composite_tuple.h"
#include "sql/expr/tuple.h"

using namespace std;
using namespace common;

GroupByVecPhysicalOperator::GroupByVecPhysicalOperator(
    std::vector<std::unique_ptr<Expression>> &&group_by_exprs, std::vector<Expression *> &&aggregate_exprs)
    : group_by_exprs_(std::move(group_by_exprs)), aggregate_exprs_(std::move(aggregate_exprs))
{
  hash_table_ = std::make_unique<StandardAggregateHashTable>(aggregate_exprs_);
}

RC GroupByVecPhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::INTERNAL;
  }

  RC rc = children_[0]->open(trx);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  Chunk child_chunk;
  while (RC::SUCCESS == (rc = children_[0]->next(child_chunk))) {
    Chunk groups_chunk;
    Chunk aggrs_chunk;

    // 计算group by表达式
    for (const auto &expr : group_by_exprs_) {
      Column column;
      expr->get_column(child_chunk, column);
      groups_chunk.add_column(std::make_unique<Column>(column.attr_type(), column.attr_len()), -1);
      memcpy(groups_chunk.column(groups_chunk.column_num() - 1).data(), column.data(), column.data_len());
    }

    // 计算聚合表达式
    for (const auto &expr : aggregate_exprs_) {
      Column column;
      expr->get_column(child_chunk, column);
      aggrs_chunk.add_column(std::make_unique<Column>(column.attr_type(), column.attr_len()), -1);
      memcpy(aggrs_chunk.column(aggrs_chunk.column_num() - 1).data(), column.data(), column.data_len());
    }

    rc = hash_table_->add_chunk(groups_chunk, aggrs_chunk);
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }

  if (rc != RC::RECORD_EOF) {
    return rc;
  }

  scanner_ = std::make_unique<StandardAggregateHashTable::Scanner>(hash_table_.get());
  scanner_->open_scan();

  return RC::SUCCESS;
}

RC GroupByVecPhysicalOperator::next(Chunk &chunk)
{
  if (is_first_next_) {
    is_first_next_ = false;
    return scanner_->next(chunk);
  }
  return RC::RECORD_EOF;
}

RC GroupByVecPhysicalOperator::close()
{
  if (scanner_) {
    scanner_->close_scan();
  }
  children_[0]->close();
  return RC::SUCCESS;
}