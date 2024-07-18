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
    : group_by_exprs_(std::move(group_by_exprs)),
      aggregate_exprs_(std::move(aggregate_exprs)),
      hash_table_(aggregate_exprs_)
{
  value_expressions_.reserve(aggregate_exprs_.size());

  std::transform(
      aggregate_exprs_.begin(), aggregate_exprs_.end(), std::back_inserter(value_expressions_), [](Expression *expr) {
        auto       *aggregate_expr = static_cast<AggregateExpr *>(expr);
        Expression *child_expr     = aggregate_expr->child().get();
        ASSERT(child_expr != nullptr, "aggregation expression must have a child expression");
        return child_expr;
      });

  // Add group by columns
  for (size_t i = 0; i < group_by_exprs_.size(); ++i) {
    add_column_to_chunk(group_by_exprs_[i].get(), i);
  }

  // Add aggr columns
  for (size_t i = 0; i < aggregate_exprs_.size(); ++i) {
    add_column_to_chunk(static_cast<AggregateExpr *>(aggregate_exprs_[i]), i + group_by_exprs_.size());
  }
}

void GroupByVecPhysicalOperator::add_column_to_chunk(const Expression *expr, size_t index)
{
  switch (expr->value_type()) {
    case AttrType::INTS: output_chunk_.add_column(make_unique<Column>(AttrType::INTS, 4), index); break;
    case AttrType::FLOATS: output_chunk_.add_column(make_unique<Column>(AttrType::FLOATS, 4), index); break;
    case AttrType::CHARS:
      output_chunk_.add_column(make_unique<Column>(AttrType::CHARS, expr->value_length()), index);
      break;
    case AttrType::BOOLEANS: output_chunk_.add_column(make_unique<Column>(AttrType::BOOLEANS, 1), index); break;
    default: ASSERT(false, "not supported type");
  }
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
  while (OB_SUCC(children_[0]->next(child_chunk))) {
    Chunk groups_chunk;
    Chunk aggrs_chunk;

    // 计算group by表达式
    for (const auto &expr : group_by_exprs_) {
      auto column = std::make_unique<Column>();
      expr->get_column(child_chunk, *column);
      groups_chunk.add_column(std::move(column), -1);
      //LOG_WARN("column: %d,rows: %d", groups_chunk.column_num(), groups_chunk.rows());
    }

    // 计算聚合表达式
    for (const auto &expr : value_expressions_) {
      auto column = std::make_unique<Column>();
      expr->get_column(child_chunk, *column);
      aggrs_chunk.add_column(std::move(column), -1);
    }

    hash_table_.add_chunk(groups_chunk, aggrs_chunk);
  }

  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;
  }

  scanner_ = new StandardAggregateHashTable::Scanner(&hash_table_);
  scanner_->open_scan();

  return rc;
}

RC GroupByVecPhysicalOperator::next(Chunk &chunk)
{
  RC rc = scanner_->next(output_chunk_);

  if (OB_FAIL(rc)) {
    return rc;
  }
  rc = chunk.reference(output_chunk_);
  return rc;
}

RC GroupByVecPhysicalOperator::close()
{
  if (scanner_) {
    scanner_->close_scan();
  }
  children_[0]->close();
  return RC::SUCCESS;
}
