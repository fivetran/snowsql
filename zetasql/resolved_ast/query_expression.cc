//
// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "zetasql/resolved_ast/query_expression.h"

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/analyzer/query_resolver_helper.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/case.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"

namespace zetasql {

// Joins entries present in <list> (with pairs as elements) separated by
// <delimiter>. While appending each pair we add the second element (if present)
// as an alias to the first element.
static std::string JoinListWithAliases(
    const std::vector<std::pair<std::string, std::string>>& list,
    absl::string_view delimiter) {
  std::string list_str;
  bool first = true;
  for (const auto& entry : list) {
    if (!first) absl::StrAppend(&list_str, delimiter);

    if (entry.second.empty()) {
      absl::StrAppend(&list_str, entry.first);
    } else {
      absl::StrAppend(&list_str, entry.first, " AS ", entry.second);
    }
    first = false;
  }
  return list_str;
}

absl::StatusOr<QueryExpression::QueryType> QueryExpression::GetQueryType()
    const {
  if (set_op_scan_list_.empty()) {
    ZETASQL_RET_CHECK(!select_list_.empty());
    ZETASQL_RET_CHECK(set_op_type_.empty());
    ZETASQL_RET_CHECK(set_op_column_match_mode_.empty());
    ZETASQL_RET_CHECK(corresponding_set_op_output_column_list_.empty());
    return QueryExpression::kDefaultQueryType;
  }

  ZETASQL_RET_CHECK(select_list_.empty());
  ZETASQL_RET_CHECK(!set_op_type_.empty());
  if (set_op_column_match_mode_.empty()) {
    ZETASQL_RET_CHECK(corresponding_set_op_output_column_list_.empty());
    return QueryExpression::kPositionalSetOpScan;
  }
  ZETASQL_RET_CHECK(!corresponding_set_op_output_column_list_.empty());
  return QueryExpression::kCorrespondenceSetOpScan;
}

void QueryExpression::set_corresponding_set_op_output_column_list(
    std::vector<std::pair<std::string, std::string>> select_list) {
  corresponding_set_op_output_column_list_ = std::move(select_list);
}

void QueryExpression::ClearAllClauses() {
  with_list_.clear();
  select_list_.clear();
  select_as_modifier_.clear();
  query_hints_.clear();
  from_.clear();
  where_.clear();
  set_op_type_.clear();
  set_op_modifier_.clear();
  set_op_column_match_mode_.clear();
  set_op_scan_list_.clear();
  corresponding_set_op_output_column_list_.clear();
  group_by_list_.clear();
  group_by_hints_.clear();
  order_by_list_.clear();
  order_by_hints_.clear();
  limit_.clear();
  offset_.clear();
  top_.clear();
  anonymization_options_.clear();
  with_recursive_ = false;
  pivot_.clear();
  unpivot_.clear();
}

std::string QueryExpression::GetSQLQuery() const {
  std::string sql;
  if (!with_list_.empty()) {
    absl::StrAppend(&sql, "WITH ");
    if (with_recursive_) {
      absl::StrAppend(&sql, "RECURSIVE ");
    }
    absl::StrAppend(&sql, JoinListWithAliases(with_list_, ", "), " ");
  }
  if (!select_list_.empty()) {
    ABSL_DCHECK(set_op_type_.empty() && set_op_modifier_.empty() &&
           set_op_scan_list_.empty());
    absl::StrAppend(&sql, "SELECT ",
                    anonymization_options_.empty()
                        ? ""
                        : absl::StrCat(anonymization_options_, " "),
                    query_hints_.empty() ? "" : absl::StrCat(query_hints_, " "),
                    top_.empty() ? "" : absl::StrCat("TOP ", top_, " "),
                    select_as_modifier_.empty()
                        ? ""
                        : absl::StrCat(select_as_modifier_, " "),
                    JoinListWithAliases(select_list_, ", "));
  }

  if (!set_op_scan_list_.empty()) {
    ABSL_DCHECK(!set_op_type_.empty());
    ABSL_DCHECK(!set_op_modifier_.empty());
    ABSL_DCHECK(select_list_.empty());
    ABSL_DCHECK(from_.empty() && where_.empty() && group_by_list_.empty());
    for (int i = 0; i < set_op_scan_list_.size(); ++i) {
      QueryExpression* qe = set_op_scan_list_[i].get();
      if (!select_as_modifier_.empty()) {
        if (qe->select_as_modifier_.empty()) {
          qe->SetSelectAsModifier(select_as_modifier_);
        } else {
          ABSL_DCHECK_EQ(qe->select_as_modifier_, select_as_modifier_);
        }
      }
      if (i > 0) {
        absl::StrAppend(&sql, " ", set_op_type_);
        if (i == 1) {
          absl::StrAppend(&sql, " ", query_hints_);
        }
        absl::StrAppend(&sql, " ", set_op_modifier_);
        if (!set_op_column_match_mode_.empty()) {
          absl::StrAppend(&sql, " ", set_op_column_match_mode_);
        }
      }
      absl::StrAppend(&sql, "(", qe->GetSQLQuery(), ")");
    }
  }

  if (!from_.empty()) {
    absl::StrAppend(&sql, " FROM ", from_);
  }

  if (!pivot_.empty()) {
    absl::StrAppend(&sql, pivot_);
  }
  if (!unpivot_.empty()) {
    absl::StrAppend(&sql, unpivot_);
  }

  if (!where_.empty()) {
    absl::StrAppend(&sql, " WHERE ", where_);
  }

  if (!group_by_list_.empty()) {
    absl::StrAppend(
        &sql, " GROUP ",
        group_by_hints_.empty() ? "" : absl::StrCat(group_by_hints_, " "),
        "BY ");
    // Legacy ROLLUP
    if (!rollup_column_id_list_.empty()) {
      absl::StrAppend(
          &sql, "ROLLUP(",
          absl::StrJoin(rollup_column_id_list_, ", ",
                        [this](std::string* out, int column_id) {
                          absl::StrAppend(
                              out, zetasql_base::FindOrDie(group_by_list_, column_id));
                        }),
          ")");
    } else if (!grouping_set_id_list_.empty()) {
      // There are rollup, cube, or grouping sets in the group by clause.
      // a lambda expression to output a column list
      auto append_column_list = [this](std::string* output,
                                       const std::vector<int>& column_id_list) {
        if (column_id_list.empty()) {
          absl::StrAppend(output, "()");
          return;
        }
        if (column_id_list.size() > 1) {
          absl::StrAppend(output, "(");
        }
        absl::StrAppend(
            output,
            absl::StrJoin(column_id_list, ", ",
                          [this](std::string* out, int column_id) {
                            absl::StrAppend(
                                out, zetasql_base::FindOrDie(group_by_list_, column_id));
                          }));
        if (column_id_list.size() > 1) {
          absl::StrAppend(output, ")");
        }
      };
      std::vector<std::string> grouping_set_strs;
      for (const GroupingSetIds& grouping_set_ids : grouping_set_id_list_) {
        std::string grouping_set_str = "";
        if (grouping_set_ids.kind == GroupingSetKind::kGroupingSet) {
          std::vector<int> column_id_list;
          for (const std::vector<int>& multi_column : grouping_set_ids.ids) {
            ABSL_DCHECK_EQ(multi_column.size(), 1);
            column_id_list.push_back(multi_column.front());
          }
          append_column_list(&grouping_set_str, column_id_list);
        } else if (grouping_set_ids.kind == GroupingSetKind::kRollup ||
                   grouping_set_ids.kind == GroupingSetKind::kCube) {
          absl::StrAppend(&grouping_set_str,
                          grouping_set_ids.kind == GroupingSetKind::kRollup
                              ? "ROLLUP"
                              : "CUBE",
                          "(");
          std::vector<std::string> multi_column_strs;
          ABSL_DCHECK_GT(grouping_set_ids.ids.size(), 0);
          for (const std::vector<int>& multi_column : grouping_set_ids.ids) {
            ABSL_DCHECK_GT(multi_column.size(), 0);
            std::string multi_column_str = "";
            append_column_list(&multi_column_str, multi_column);
            multi_column_strs.push_back(multi_column_str);
          }
          absl::StrAppend(&grouping_set_str,
                          absl::StrJoin(multi_column_strs, ", "));
          absl::StrAppend(&grouping_set_str, ")");
        }
        grouping_set_strs.push_back(grouping_set_str);
      }
      // Wrap grouping sets strings to GROUPING SETS only when
      // 1. Multiple grouping sets, e.g. GROUPING SETS(x, ROLLUP(y)), OR
      // 2. A grouping set, but its kind is kGroupingSet, e.g. GROUPING SETS(x)
      // Otherwise for simplicity, we generate a top-level ROLLUP or CUBE
      // instead of wrapping it to GROUPING SETS. E.g. We generate ROLLUP(x, y)
      // rather than GROUPING SETS(ROLLUP(x, y)) where there is only a rollup.
      if (grouping_set_strs.size() > 1 ||
          grouping_set_id_list_.front().kind == GroupingSetKind::kGroupingSet) {
        absl::StrAppend(&sql, " GROUPING SETS(",
                        absl::StrJoin(grouping_set_strs, ", "), ")");
      } else {
        absl::StrAppend(&sql, " ", grouping_set_strs.front());
      }
    } else {
      // We assume while iterating the group_by_list_, the entries will be
      // sorted by the column id.
      absl::StrAppend(
          &sql,
          absl::StrJoin(
              group_by_list_, ", ",
              [](std::string* out,
                 const std::pair<int, std::string>& column_id_and_string) {
                absl::StrAppend(out, column_id_and_string.second);
              }));
    }
  }

  if (!order_by_list_.empty()) {
    absl::StrAppend(
        &sql, " ORDER ",
        order_by_hints_.empty() ? "" : absl::StrCat(order_by_hints_, " "),
        "BY ", absl::StrJoin(order_by_list_, ", "));
  }

  if (!limit_.empty()) {
    absl::StrAppend(&sql, " LIMIT ", limit_);
  }

  if (!offset_.empty()) {
    absl::StrAppend(&sql, " OFFSET ", offset_);
  }

  return sql;
}

bool QueryExpression::CanFormSQLQuery() const {
  return !CanSetSelectClause();
}

void QueryExpression::Wrap(absl::string_view alias) {
  ABSL_DCHECK(CanFormSQLQuery());
  ABSL_DCHECK(!alias.empty());
  const std::string sql = GetSQLQuery();
  ClearAllClauses();
  from_ = absl::StrCat("(", sql, ") AS ", alias);
}

bool QueryExpression::TrySetWithClause(
    const std::vector<std::pair<std::string, std::string>>& with_list,
    bool recursive) {
  if (!CanSetWithClause()) {
    return false;
  }
  with_list_ = with_list;
  with_recursive_ = recursive;
  return true;
}

bool QueryExpression::TrySetSelectClause(
    const std::vector<std::pair<std::string, std::string>>& select_list,
    const std::string& select_hints) {
  if (!CanSetSelectClause()) {
    return false;
  }
  select_list_ = select_list;
  ABSL_DCHECK(query_hints_.empty());
  query_hints_ = select_hints;
  return true;
}

void QueryExpression::ResetSelectClause() {
  select_list_.clear();
}

bool QueryExpression::TrySetFromClause(const std::string& from) {
  if (!CanSetFromClause()) {
    return false;
  }
  from_ = from;
  return true;
}

bool QueryExpression::TrySetWhereClause(const std::string& where) {
  if (!CanSetWhereClause()) {
    return false;
  }
  where_ = where;
  return true;
}

bool QueryExpression::TrySetSetOpScanList(
    std::vector<std::unique_ptr<QueryExpression>>* set_op_scan_list,
    const std::string& set_op_type, const std::string& set_op_modifier,
    const std::string& set_op_column_match_mode,
    const std::string& query_hints) {
  if (!CanSetSetOpScanList()) {
    return false;
  }
  ABSL_DCHECK(set_op_scan_list != nullptr);
  set_op_scan_list_ = std::move(*set_op_scan_list);
  set_op_scan_list->clear();
  ABSL_DCHECK(set_op_type_.empty());
  ABSL_DCHECK(set_op_modifier_.empty());
  set_op_type_ = set_op_type;
  set_op_modifier_ = set_op_modifier;
  set_op_column_match_mode_ = set_op_column_match_mode;
  query_hints_ = query_hints;
  return true;
}

bool QueryExpression::TrySetGroupByClause(
    const std::map<int, std::string>& group_by_list,
    const std::string& group_by_hints,
    const std::vector<GroupingSetIds>& grouping_set_id_list,
    const std::vector<int>& rollup_column_id_list) {
  if (!CanSetGroupByClause()) {
    return false;
  }
  group_by_list_ = group_by_list;
  ABSL_DCHECK(group_by_hints_.empty());
  group_by_hints_ = group_by_hints;
  grouping_set_id_list_ = grouping_set_id_list;
  rollup_column_id_list_ = rollup_column_id_list;
  return true;
}

bool QueryExpression::TrySetOrderByClause(
    const std::vector<std::string>& order_by_list,
    const std::string& order_by_hints) {
  if (!CanSetOrderByClause()) {
    return false;
  }
  order_by_list_ = order_by_list;
  ABSL_DCHECK(order_by_hints_.empty());
  order_by_hints_ = order_by_hints;
  return true;
}

bool QueryExpression::TrySetLimitClause(const std::string& limit) {
  if (!CanSetLimitClause()) {
    return false;
  }
  limit_ = limit;
  return true;
}

bool QueryExpression::TrySetOffsetClause(const std::string& offset) {
  if (!CanSetOffsetClause()) {
    return false;
  }
  offset_ = offset;
  return true;
}

bool QueryExpression::TrySetTopClause(const std::string& top) {
  if (!CanSetTopClause()) {
    return false;
  }
  top_ = top;
  return true;
}

bool QueryExpression::TrySetWithAnonymizationClause(
    const std::string& anonymization_options) {
  if (!CanSetWithAnonymizationClause()) {
    return false;
  }
  anonymization_options_ = anonymization_options;
  return true;
}

bool QueryExpression::TrySetPivotClause(const std::string& pivot) {
  if (!CanSetPivotClause()) {
    return false;
  }
  pivot_ = pivot;
  return true;
}

bool QueryExpression::TrySetUnpivotClause(const std::string& unpivot) {
  if (!CanSetUnpivotClause()) {
    return false;
  }
  unpivot_ = unpivot;
  return true;
}

bool QueryExpression::CanSetWithClause() const {
  return !HasWithClause();
}
bool QueryExpression::CanSetSelectClause() const {
  return !HasSelectClause() && !HasSetOpScanList();
}
bool QueryExpression::CanSetFromClause() const {
  return !HasFromClause() && CanSetSelectClause();
}
bool QueryExpression::CanSetWhereClause() const {
  return !HasWhereClause() && HasFromClause() && CanSetSelectClause();
}
bool QueryExpression::CanSetSetOpScanList() const {
  return !HasSetOpScanList() && !HasSelectClause() && !HasFromClause() &&
         !HasWhereClause() && !HasGroupByClause();
}
bool QueryExpression::CanSetGroupByClause() const {
  return !HasGroupByClause() && HasFromClause() && CanSetSelectClause();
}
bool QueryExpression::CanSetOrderByClause() const {
  return !HasOrderByClause() && !HasLimitClause() && !HasOffsetClause() &&
      HasFromClause();
}
bool QueryExpression::CanSetLimitClause() const {
  return !HasLimitClause() && !HasOffsetClause();
}
bool QueryExpression::CanSetOffsetClause() const { return !HasOffsetClause(); }

bool QueryExpression::CanSetTopClause() const {
  return !HasTopClause() && !HasLimitClause() && !HasOffsetClause();
}

bool QueryExpression::CanSetPivotClause() const { return !HasPivotClause(); }

bool QueryExpression::CanSetUnpivotClause() const {
  return !HasUnpivotClause();
}

const std::vector<std::pair<std::string, std::string>>&
QueryExpression::SelectList() const {
  if (!set_op_scan_list_.empty()) {
    ABSL_DCHECK(select_list_.empty());
    if (!set_op_column_match_mode_.empty()) {
      return corresponding_set_op_output_column_list_;
    }
    return set_op_scan_list_[0]->SelectList();
  }

  return select_list_;
}
bool QueryExpression::CanSetWithAnonymizationClause() const {
  return !HasWithAnonymizationClause();
}

static bool HasDuplicateAliases(
    const absl::flat_hash_map<int, absl::string_view>& aliases) {
  absl::flat_hash_set<absl::string_view, zetasql_base::StringViewCaseHash,
                      zetasql_base::StringViewCaseEqual>
      seen_aliases;
  for (const auto [index, alias] : aliases) {
    if (!seen_aliases.insert(alias).second) {
      return true;
    }
  }
  return false;
}

absl::Status QueryExpression::SetAliasesForSelectList(
    const absl::flat_hash_map<int, absl::string_view>& aliases) {
  ZETASQL_ASSIGN_OR_RETURN(QueryType type, GetQueryType());
  switch (type) {
    case kDefaultQueryType:
      for (const auto [index, alias] : aliases) {
        ZETASQL_RET_CHECK_LT(index, select_list_.size());
        select_list_[index].second = alias;
      }
      break;
    case kPositionalSetOpScan:
      ZETASQL_RETURN_IF_ERROR(set_op_scan_list_[0]->SetAliasesForSelectList(aliases));
      break;
    case kCorrespondenceSetOpScan:
      ZETASQL_RET_CHECK(!HasDuplicateAliases(aliases));
      absl::flat_hash_map<absl::string_view, absl::string_view>
          old_to_new_alias;
      for (const auto [index, new_alias] : aliases) {
        ZETASQL_RET_CHECK_LT(index, corresponding_set_op_output_column_list_.size());
        ZETASQL_RET_CHECK(
            old_to_new_alias
                .insert({corresponding_set_op_output_column_list_[index].second,
                         new_alias})
                .second);
      }
      // Recursively set aliases for each set operation item.
      for (int item_index = 0; item_index < set_op_scan_list_.size();
           ++item_index) {
        absl::flat_hash_map<int, absl::string_view> item_aliases;
        for (int col_idx = 0;
             col_idx < set_op_scan_list_[item_index]->SelectList().size();
             ++col_idx) {
          absl::string_view old_alias =
              set_op_scan_list_[item_index]->SelectList()[col_idx].second;
          const auto new_alias = old_to_new_alias.find(old_alias);
          if (new_alias != old_to_new_alias.end()) {
            item_aliases[col_idx] = new_alias->second;
          }
        }
        ZETASQL_RETURN_IF_ERROR(set_op_scan_list_[item_index]->SetAliasesForSelectList(
            item_aliases));
      }
      // Update the select list for CORRESPONDING.
      for (const auto [index, new_alias] : aliases) {
        corresponding_set_op_output_column_list_[index].second = new_alias;
      }
      break;
  }
  return absl::OkStatus();
}

void QueryExpression::SetSelectAsModifier(const std::string& modifier) {
  ABSL_DCHECK(select_as_modifier_.empty());
  select_as_modifier_ = modifier;
}

}  // namespace zetasql
