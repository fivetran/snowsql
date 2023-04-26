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

#include "zetasql/analyzer/anonymization_rewriter.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "google/protobuf/descriptor.h"
#include "zetasql/analyzer/expr_matching_helpers.h"
#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/analyzer/named_argument_info.h"
#include "zetasql/analyzer/query_resolver_helper.h"
#include "zetasql/analyzer/resolver.h"
#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/proto/anon_output_with_report.pb.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/anon_function.h"
#include "zetasql/public/anonymization_utils.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/proto_util.h"
#include "zetasql/public/select_with_mode.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/make_node_vector.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

struct WithEntryRewriteState;
struct RewritePerUserTransformResult;
struct UidColumnState;

// Used for generating correct error messages for SELECT WITH ANONYMIZATION and
// SELECT WITH DIFFERENTIAL_PRIVACY.
struct SelectWithModeName {
  absl::string_view name;
  // Article used with name if true should use `a`.
  bool uses_a_article;
};
// Rewrites a given AST that includes a ResolvedAnonymizedAggregateScan to use
// the semantics defined in https://arxiv.org/abs/1909.01917 and
// (broken link).
//
// Overview of the rewrite process:
// 1. This class is invoked on an AST node, blindly copying everything until a
//    ResolvedAnonymizedAggregateScan (anon node) is hit
// 2. Every column in the anon node's column list is recorded in a map entry
//    with a freshly allocated column of the same type in the entry's value
//    (the intermediate columns)
// 3. The per-user ResolvedAggregateScan is created using this map:
//   a. The original anon node's input scan is validated to partition by $uid,
//      and project the $uid column up to the top column list
//   b. The projected $uid column is added to the GROUP BY list if not already
//      included.
//   c. Each ANON_* function call in the anon node is re-resolved to the
//      appropriate per-user aggregate function, e.g. ANON_SUM(expr)->SUM(expr)
//   d. For each aggregate or group by column in the anon node, the column set
//      in the per-user scan's column list is the appropriate intermediate
//      column looked up in the column map
// 4. If max_groups_contributed (aka kappa) is specified, a partitioned-by-$uid
//    ResolvedSampleScan is inserted to limit the number of groups that a user
//    can contribute to. While max_groups_contributed is optional, for most
//    queries with a GROUP BY clause in the ResolvedAnonymizedAggregationScan it
//    MUST be specified for the resulting query to provide correct epsilon-delta
//    differential privacy.
// 5. The final cross-user ResolvedAnonymizedAggregateScan is created:
//   a. The input scan is set to the (possibly sampled) per-user scan
//   b. The first argument for each ANON_* function call in the anon node is
//      re-resolved to point to the appropriate intermediate column
//   c. A group selection threshold computing ANON_COUNT(*) function call is
//      added
//
// If we consider the scans in the original AST as a linked list as:
//
// cross_user_transform
//  -> ResolvedAnonymizedAggregateScan
//    -> per_user_transform
//
// Then the above operations can be thought of as inserting a pair of new list
// nodes:
//
// cross_user_transform
//  -> ResolvedAnonymizedAggregateScan
//    -> ResolvedSampleScan (optional)
//      -> ResolvedAggregateScan
//        -> per_user_transform
//
// Where the new ResolvedAggregateScan is the per-user aggregate scan, and
// the optional ResolvedSampleScan uses max_groups_contributed to restrict the
// number of groups a user can contribute to (for more information on
// max_groups_contributed, see (broken link)).
class RewriterVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  RewriterVisitor(ColumnFactory* allocator, TypeFactory* type_factory,
                  Resolver* resolver,
                  RewriteForAnonymizationOutput::TableScanToAnonAggrScanMap&
                      table_scan_to_anon_aggr_scan_map,
                  RewriteForAnonymizationOutput::TableScanToDPAggrScanMap&
                      table_scan_to_dp_aggr_scan_map,
                  Catalog* catalog, AnalyzerOptions* options)
      : allocator_(allocator),
        type_factory_(type_factory),
        resolver_(resolver),
        catalog_(catalog),
        analyzer_options_(options),
        table_scan_to_anon_aggr_scan_map_(table_scan_to_anon_aggr_scan_map),
        table_scan_to_dp_aggr_scan_map_(table_scan_to_dp_aggr_scan_map) {}

 private:
  // Chooses one of the uid columns between per_user_visitor_uid_column and
  // options_uid. If both are present returns an error if none is present
  // returns an error.
  absl::StatusOr<std::unique_ptr<const ResolvedExpr>> ChooseUidColumn(
      const ResolvedAggregateScanBase* node,
      SelectWithModeName select_with_mode_name,
      const UidColumnState& per_user_visitor_uid_column_state,
      std::optional<const ResolvedExpr*> options_uid_column);

  absl::StatusOr<RewritePerUserTransformResult> RewritePerUserTransform(
      const ResolvedAggregateScanBase* node,
      SelectWithModeName select_with_mode_name,
      std::optional<const ResolvedExpr*> options_uid_column);

  // Create the cross-user group selection threshold function call. It is called
  // k_threshold for ResolvedAnonymizedAggregateScan but the name got updated to
  // group selection threshold see: (broken link).
  absl::StatusOr<std::unique_ptr<ResolvedComputedColumn>>
  MakeGroupSelectionThresholdFunctionColumn(
      const ResolvedAnonymizedAggregateScan* scan_node);
  absl::StatusOr<std::unique_ptr<ResolvedComputedColumn>>
  MakeGroupSelectionThresholdFunctionColumn(
      const ResolvedDifferentialPrivacyAggregateScan* scan_node);

  std::unique_ptr<ResolvedAnonymizedAggregateScan>
  CreateAggregateScanAndUpdateScanMap(
      const ResolvedAnonymizedAggregateScan* node,
      std::unique_ptr<ResolvedScan> input_scan,
      std::vector<std::unique_ptr<ResolvedComputedColumn>> outer_group_by_list,
      std::vector<std::unique_ptr<ResolvedComputedColumn>> outer_aggregate_list,
      std::unique_ptr<ResolvedExpr> group_selection_threshold_expr,
      std::vector<std::unique_ptr<ResolvedOption>> resolved_options);

  std::unique_ptr<ResolvedDifferentialPrivacyAggregateScan>
  CreateAggregateScanAndUpdateScanMap(
      const ResolvedDifferentialPrivacyAggregateScan* node,
      std::unique_ptr<ResolvedScan> input_scan,
      std::vector<std::unique_ptr<ResolvedComputedColumn>> outer_group_by_list,
      std::vector<std::unique_ptr<ResolvedComputedColumn>> outer_aggregate_list,
      std::unique_ptr<ResolvedExpr> group_selection_threshold_expr,
      std::vector<std::unique_ptr<ResolvedOption>> resolved_options);

  // Wraps input_scan with a sample scan that bounds the number of partitions
  // that a user contributes to.
  //
  // This will only provide epsilon-delta dataset level differential privacy
  // when the query includes a GROUP BY clause.
  //
  // If max_groups_contributed is explicitly set to NULL, then we don't add a
  // SampleScan. If max_groups_contributed is not specified, then we add a
  // SampleScan using default_anon_kappa_value.
  absl::StatusOr<std::unique_ptr<ResolvedScan>> AddCrossPartitionSampleScan(
      std::unique_ptr<ResolvedScan> input_scan,
      std::optional<Value> max_groups_contributed,
      absl::string_view max_groups_contributed_option_name,
      ResolvedColumn uid_column,
      std::vector<std::unique_ptr<ResolvedOption>>&
          resolved_anonymization_options);

  absl::Status VisitResolvedAnonymizedAggregateScan(
      const ResolvedAnonymizedAggregateScan* node) override {
    return VisitResolvedDifferentialPrivacyAggregateScanTemplate(node);
  }
  absl::Status VisitResolvedDifferentialPrivacyAggregateScan(
      const ResolvedDifferentialPrivacyAggregateScan* node) override {
    return VisitResolvedDifferentialPrivacyAggregateScanTemplate(node);
  }

  template <class NodeType>
  absl::Status VisitResolvedDifferentialPrivacyAggregateScanTemplate(
      const NodeType* node);

  absl::Status VisitResolvedWithScan(const ResolvedWithScan* node) override;
  absl::Status VisitResolvedProjectScan(
      const ResolvedProjectScan* node) override;

  absl::Status AttachExtraNodeFields(const ResolvedScan& original,
                                     ResolvedScan& copy);

  ColumnFactory* allocator_;           // unowned
  TypeFactory* type_factory_;          // unowned
  Resolver* resolver_;                 // unowned
  Catalog* catalog_;                   // unowned
  AnalyzerOptions* analyzer_options_;  // unowned
  RewriteForAnonymizationOutput::TableScanToAnonAggrScanMap&
      table_scan_to_anon_aggr_scan_map_;
  RewriteForAnonymizationOutput::TableScanToDPAggrScanMap&
      table_scan_to_dp_aggr_scan_map_;
  std::vector<const ResolvedTableScan*> resolved_table_scans_;  // unowned
  std::vector<std::unique_ptr<WithEntryRewriteState>> with_entries_;
};

// Use the resolver to create a new function call using resolved arguments. The
// calling code must ensure that the arguments can always be coerced and
// resolved to a valid function. Any returned status is an internal error.
absl::StatusOr<std::unique_ptr<ResolvedExpr>> ResolveFunctionCall(
    const std::string& function_name,
    std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
    std::vector<NamedArgumentInfo> named_arguments, Resolver* resolver) {
  // In order for the resolver to provide error locations, it needs ASTNode
  // locations from the original SQL. However, the functions in these
  // transforms do not necessarily appear in the SQL so they have no locations.
  // Any errors produced here are internal errors, so error locations are not
  // meaningful and we use location stubs instead.
  ASTFunctionCall dummy_ast_function;
  FakeASTNode dummy_ast_location;
  std::vector<const ASTNode*> dummy_arg_locations(arguments.size(),
                                                  &dummy_ast_location);

  // Stub out query/expr resolution info structs. This is ok because we aren't
  // doing any actual resolution here (so we don't need NameScopes, etc.). We
  // are just transforming a function call, and creating a new
  // ResolvedFunctionCall with already-resolved arguments.
  NameScope empty_name_scope;
  QueryResolutionInfo query_resolution_info(resolver);
  ExprResolutionInfo expr_resolution_info(
      &empty_name_scope, &empty_name_scope, &empty_name_scope,
      /*allows_aggregation_in=*/true,
      /*allows_analytic_in=*/false, /*use_post_grouping_columns_in=*/false,
      /*clause_name_in=*/"", &query_resolution_info);

  std::unique_ptr<const ResolvedExpr> result;
  absl::Status status = resolver->ResolveFunctionCallWithResolvedArguments(
      &dummy_ast_function, dummy_arg_locations, function_name,
      std::move(arguments), std::move(named_arguments), &expr_resolution_info,
      &result);

  // We expect that the caller passes valid/coercible arguments. An error only
  // occurs if that contract is violated, so this is an internal error.
  ZETASQL_RET_CHECK(status.ok()) << status;

  // The resolver inserts the actual function call for aggregate functions
  // into query_resolution_info, so we need to extract it if applicable.
  if (query_resolution_info.aggregate_columns_to_compute().size() == 1) {
    std::unique_ptr<ResolvedComputedColumn> col =
        absl::WrapUnique(const_cast<ResolvedComputedColumn*>(
            query_resolution_info.release_aggregate_columns_to_compute()
                .front()
                .release()));
    result = col->release_expr();
  }
  return absl::WrapUnique(const_cast<ResolvedExpr*>(result.release()));
}

std::unique_ptr<ResolvedColumnRef> MakeColRef(const ResolvedColumn& col) {
  return MakeResolvedColumnRef(col.type(), col, /*is_correlated=*/false);
}

zetasql_base::StatusBuilder MakeSqlErrorAtNode(const ResolvedNode& node) {
  zetasql_base::StatusBuilder builder = MakeSqlError();
  const auto* parse_location = node.GetParseLocationRangeOrNULL();
  if (parse_location != nullptr) {
    builder.Attach(parse_location->start().ToInternalErrorLocation());
  }
  return builder;
}

absl::Status MaybeAttachParseLocation(absl::Status status,
                                      const ResolvedNode& node) {
  const auto* parse_location = node.GetParseLocationRangeOrNULL();
  if (!status.ok() &&
      !zetasql::internal::HasPayloadWithType<InternalErrorLocation>(status) &&
      parse_location != nullptr) {
    zetasql::internal::AttachPayload(
        &status, parse_location->start().ToInternalErrorLocation());
  }
  return status;
}

// Return true if the internal implementation of differential privacy function
// uses array type as an input.
bool HasInnerAggregateArray(int64_t signature_id) {
  switch (signature_id) {
    case FunctionSignatureId::FN_ANON_VAR_POP_DOUBLE:
    case FunctionSignatureId::FN_ANON_STDDEV_POP_DOUBLE:
    case FunctionSignatureId::FN_ANON_PERCENTILE_CONT_DOUBLE:
    case FunctionSignatureId::FN_ANON_QUANTILES_DOUBLE:
    case FunctionSignatureId::FN_ANON_QUANTILES_DOUBLE_WITH_REPORT_JSON:
    case FunctionSignatureId::FN_ANON_QUANTILES_DOUBLE_WITH_REPORT_PROTO:
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_VAR_POP_DOUBLE:
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_STDDEV_POP_DOUBLE:
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_PERCENTILE_CONT_DOUBLE:
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_QUANTILES_DOUBLE:
    case FunctionSignatureId::
        FN_DIFFERENTIAL_PRIVACY_QUANTILES_DOUBLE_REPORT_JSON:
    case FunctionSignatureId::
        FN_DIFFERENTIAL_PRIVACY_QUANTILES_DOUBLE_REPORT_PROTO:
      return true;
    default:
      return false;
  }
}

bool IsCountStarFunction(int64_t signature_id) {
  switch (signature_id) {
    case FunctionSignatureId::FN_ANON_COUNT_STAR:
    case FunctionSignatureId::FN_ANON_COUNT_STAR_WITH_REPORT_JSON:
    case FunctionSignatureId::FN_ANON_COUNT_STAR_WITH_REPORT_PROTO:
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_COUNT_STAR:
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_COUNT_STAR_REPORT_JSON:
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_COUNT_STAR_REPORT_PROTO:
      return true;

    default:
      return false;
  }
}

// Given a call to an ANON_* function, resolve a concrete function signature for
// the matching per-user aggregate call. For example,
// ANON_COUNT(expr, 0, 1) -> COUNT(expr)
absl::StatusOr<std::unique_ptr<ResolvedExpr>>
ResolveInnerAggregateFunctionCallForAnonFunction(
    const ResolvedAggregateFunctionCall* node,
    std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
    Resolver* resolver, ResolvedColumn* order_by_column,
    ColumnFactory* allocator, absl::string_view select_with_identifier) {
  if (!node->function()->Is<AnonFunction>()) {
    return MakeSqlErrorAtNode(*node)
           << "Unsupported function in SELECT WITH " << select_with_identifier
           << " select list: " << node->function()->SQLName();
  }

  if (node->function()->GetGroup() == Function::kZetaSQLFunctionGroupName &&
      IsCountStarFunction(node->signature().context_id())) {
    // COUNT(*) doesn't take any arguments.
    arguments.clear();
  } else {
    arguments.resize(1);
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedExpr> result,
      ResolveFunctionCall(
          node->function()->GetAs<AnonFunction>()->GetPartialAggregateName(),
          std::move(arguments), /*named_arguments=*/{}, resolver));

  // If the anon function is an anon array function, we allocate a new column
  // "$orderbycol1" and set the limit as 5.
  if (node->function()->GetGroup() == Function::kZetaSQLFunctionGroupName &&
      HasInnerAggregateArray(node->signature().context_id())) {
    if (!order_by_column->IsInitialized()) {
      *order_by_column =
          allocator->MakeCol("$orderby", "$orderbycol1", types::DoubleType());
    }
    std::unique_ptr<const ResolvedColumnRef> resolved_column_ref =
        MakeColRef(*order_by_column);
    std::unique_ptr<const ResolvedOrderByItem> resolved_order_by_item =
        MakeResolvedOrderByItem(std::move(resolved_column_ref), nullptr,
                                /*is_descending=*/false,
                                ResolvedOrderByItemEnums::ORDER_UNSPECIFIED);

    ResolvedAggregateFunctionCall* resolved_aggregate_function_call =
        result->GetAs<ResolvedAggregateFunctionCall>();
    resolved_aggregate_function_call->add_order_by_item_list(
        std::move(resolved_order_by_item));
    resolved_aggregate_function_call->set_null_handling_modifier(
        ResolvedNonScalarFunctionCallBaseEnums::IGNORE_NULLS);
    resolved_aggregate_function_call->set_limit(MakeResolvedLiteral(
        Value::Int64(anonymization::kPerUserArrayAggLimit)));
  }
  return result;
}

// Rewrites the aggregate and group by list for the inner per-user aggregate
// scan. Replaces all function calls with their non-ANON_* versions, and sets
// the output column for each ComputedColumn to the corresponding intermediate
// column in the <injected_col_map>.
class InnerAggregateListRewriterVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  InnerAggregateListRewriterVisitor(
      std::map<ResolvedColumn, ResolvedColumn>* injected_col_map,
      ColumnFactory* allocator, Resolver* resolver,
      absl::string_view select_with_identifier)
      : injected_col_map_(injected_col_map),
        allocator_(allocator),
        resolver_(resolver),
        select_with_identifier_(select_with_identifier) {}

  const ResolvedColumn& order_by_column() { return order_by_column_; }

  // Rewrite the aggregates in `node` to change ANON_* functions to their
  // per-user aggregate alternatives (e.g. ANON_SUM->SUM).
  //
  // This also changes the output column of each function to the appropriate
  // intermediate column, as dictated by the injected_col_map.
  absl::StatusOr<std::vector<std::unique_ptr<ResolvedComputedColumn>>>
  RewriteAggregateColumns(const ResolvedAggregateScanBase* node) {
    std::vector<std::unique_ptr<ResolvedComputedColumn>> inner_aggregate_list;
    for (const auto& col : node->aggregate_list()) {
      ZETASQL_RETURN_IF_ERROR(col->Accept(this));
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedComputedColumn> unique_ptr_node,
                       this->ConsumeRootNode<ResolvedComputedColumn>());
      inner_aggregate_list.emplace_back(std::move(unique_ptr_node));
    }
    return inner_aggregate_list;
  }

  // Rewrite the GROUP BY list of `node` to change each output column to the
  // appropriate intermediate column, as dictated by the injected_col_map.
  //
  // Any complex GROUP BY transforms/computed columns will be included here
  // (e.g. GROUP BY col1 + col2).
  absl::StatusOr<std::vector<std::unique_ptr<ResolvedComputedColumn>>>
  RewriteGroupByColumns(const ResolvedAggregateScanBase* node) {
    std::vector<std::unique_ptr<ResolvedComputedColumn>> inner_group_by_list;
    for (const auto& col : node->group_by_list()) {
      ZETASQL_RETURN_IF_ERROR(col->Accept(this));
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedComputedColumn> unique_ptr_node,
                       this->ConsumeRootNode<ResolvedComputedColumn>());
      inner_group_by_list.emplace_back(std::move(unique_ptr_node));
    }
    return inner_group_by_list;
  }

 private:
  absl::Status VisitResolvedAggregateFunctionCall(
      const ResolvedAggregateFunctionCall* node) override {
    // Blindly copy the argument list.
    ZETASQL_ASSIGN_OR_RETURN(std::vector<std::unique_ptr<ResolvedExpr>> argument_list,
                     ProcessNodeList(node->argument_list()));

    // Trim the arg list and resolve the per-user aggregate function.
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<ResolvedExpr> result,
        ResolveInnerAggregateFunctionCallForAnonFunction(
            node,
            // This is expecting unique_ptr to be const.
            // std::vector<std::unique_ptr<__const__ ResolvedExpr>>
            {std::make_move_iterator(argument_list.begin()),
             std::make_move_iterator(argument_list.end())},
            resolver_, &order_by_column_, allocator_, select_with_identifier_));
    ZETASQL_RET_CHECK_EQ(result->node_kind(), RESOLVED_AGGREGATE_FUNCTION_CALL)
        << result->DebugString();
    PushNodeToStack(std::move(result));
    return absl::OkStatus();
  }

  absl::Status VisitResolvedComputedColumn(
      const ResolvedComputedColumn* node) override {
    // Rewrite the output column to point to the mapped column.
    ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedComputedColumn(node));
    ResolvedComputedColumn* col =
        GetUnownedTopOfStack<ResolvedComputedColumn>();

    // Create a column to splice together the per-user and cross-user
    // aggregate/groupby lists, then update the copied computed column and place
    // our new column in the replacement map.
    const ResolvedColumn& old_column = node->column();
    const ResolvedColumn injected_column = allocator_->MakeCol(
        old_column.table_name(), old_column.name() + "_partial",
        col->expr()->type());
    injected_col_map_->emplace(old_column, injected_column);
    col->set_column(injected_column);
    return absl::OkStatus();
  }

  std::map<ResolvedColumn, ResolvedColumn>* injected_col_map_;
  ColumnFactory* allocator_;
  Resolver* resolver_;
  ResolvedColumn order_by_column_;
  absl::string_view select_with_identifier_;
};

// Given a call to an ANON_* function, resolve an aggregate function call for
// use in the outer cross-user aggregation scan. This function will always be an
// ANON_* function, and the first argument will always point to the appropriate
// column produced by the per-user scan (target_column).
absl::StatusOr<std::unique_ptr<ResolvedExpr>>
ResolveOuterAggregateFunctionCallForAnonFunction(
    const ResolvedAggregateFunctionCall* node,
    const ResolvedColumn& target_column,
    std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
    Resolver* resolver) {
  // Most ANON_* functions don't require special handling.
  std::string target = node->function()->Name();
  // But ANON_COUNT(*) and ANON_COUNT(expr) require special handling. Note that
  // we implement ANON_COUNT(*) and ANON_COUNT(expr) using ANON_SUM(expr) in the
  // outer cross-user aggregation scan.
  // ANON_COUNT(*) is therefore effectively ANON_SUM(COUNT(*))
  std::vector<NamedArgumentInfo> named_arguments;
  static const IdString contribution_bounds_per_group =
      IdString::MakeGlobal("contribution_bounds_per_group");
  static const IdString report_format = IdString::MakeGlobal("report_format");
  auto id_string_pool = resolver->analyzer_options().id_string_pool();
  if (node->function()->GetGroup() == Function::kZetaSQLFunctionGroupName) {
    switch (node->signature().context_id()) {
      case FunctionSignatureId::FN_ANON_COUNT_STAR:
        // Insert a dummy 'expr' column here, the original call will not include
        // one because we are rewriting ANON_COUNT(*) to ANON_SUM(expr). The
        // actual column reference will be set below.
        arguments.insert(arguments.begin(), nullptr);
        ABSL_FALLTHROUGH_INTENDED;
      case FunctionSignatureId::FN_ANON_COUNT:
        target = "anon_sum";
        break;

      case FunctionSignatureId::FN_ANON_COUNT_STAR_WITH_REPORT_JSON:
        // Insert a dummy 'expr' column here, the original call will not include
        // one because we are rewriting ANON_COUNT(*) WITH REPORT(FORMAT=JSON)
        // to ANON_SUM(expr) WITH REPORT(FORMAT=JSON). The actual column
        // reference will be set below.
        arguments.insert(arguments.begin(), nullptr);
        ABSL_FALLTHROUGH_INTENDED;
      case FunctionSignatureId::FN_ANON_COUNT_WITH_REPORT_JSON:
        target = "$anon_sum_with_report_json";
        break;

      case FunctionSignatureId::FN_ANON_COUNT_STAR_WITH_REPORT_PROTO:
        target = "$anon_sum_with_report_proto";
        // Insert a dummy 'expr' column here, the original call will not include
        // one because we are rewriting ANON_COUNT(*) WITH REPORT(FORMAT=PROTO)
        // to ANON_SUM(expr) WITH REPORT(FORMAT=PROTO). The actual column
        // reference will be set below.
        arguments.insert(arguments.begin(), nullptr);
        ABSL_FALLTHROUGH_INTENDED;
      case FunctionSignatureId::FN_ANON_COUNT_WITH_REPORT_PROTO:
        target = "$anon_sum_with_report_proto";
        break;

      case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_COUNT_STAR:
        // Insert a dummy 'expr' column here, the original call will not include
        // one because we are rewriting dp COUNT(*) to dp SUM(expr). The actual
        // column reference will be set below.
        arguments.insert(arguments.begin(), nullptr);
        ABSL_FALLTHROUGH_INTENDED;
      case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_COUNT:
        target = "$differential_privacy_sum";
        named_arguments.emplace_back(contribution_bounds_per_group, 1, node);
        break;

      case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_COUNT_STAR_REPORT_JSON:
      case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_COUNT_STAR_REPORT_PROTO:
        // Insert a dummy 'expr' column here, the original call will not include
        // one because we are rewriting differential privacy COUNT(*,
        // report_format=<format>) to SUM(expr, report_format=<format>). The
        // actual column reference will be set below.
        arguments.insert(arguments.begin(), nullptr);
        ABSL_FALLTHROUGH_INTENDED;
      case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_COUNT_REPORT_JSON:
      case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_COUNT_REPORT_PROTO:
        target = "$differential_privacy_sum";
        named_arguments.emplace_back(report_format, 1, node);
        named_arguments.emplace_back(contribution_bounds_per_group, 2, node);
        break;

      default:
        for (int i = 0; i < arguments.size(); ++i) {
          const auto& arg = node->signature().argument(i);
          if (arg.options().named_argument_kind() ==
              FunctionEnums::NAMED_ONLY) {
            named_arguments.emplace_back(
                id_string_pool->Make(arg.argument_name()), i, node);
          }
        }
        break;
    }
  }
  // The first argument will _always_ point to the partially aggregated column
  // produced by the corresponding function call in the per-user scan.
  arguments[0] = MakeColRef(target_column);

  return ResolveFunctionCall(target, std::move(arguments),
                             std::move(named_arguments), resolver);
}

// Converts value from int64_t to Value object based on provided type.
// Returns invalid Value if the provided type isn't one of {INT64, UINT64,
// NUMERIC}.
Value ToIntValueOrInvalid(const Type& type, int64_t value) {
  switch (type.kind()) {
    case TYPE_INT64:
      return Value::Int64(value);
    case TYPE_UINT64:
      return Value::Uint64(value);
    case TYPE_NUMERIC:
      return values::Numeric(value);
    default:
      return Value::Invalid();
  }
}

// Returns true if a given expr is a literal and its value equals to
// expected_value.
//
// Note, that the type of <expr> must be Int64, Uint64 or Numeric,
// otherwise an internal error is returned.
absl::StatusOr<bool> IsLiteralWithValueEqualTo(const ResolvedExpr& expr,
                                               int64_t expected_value) {
  if (expr.node_kind() != RESOLVED_LITERAL) {
    return false;
  }
  const Value expected = ToIntValueOrInvalid(*expr.type(), expected_value);
  ZETASQL_RET_CHECK(expected.is_valid());

  const Value& literal = expr.GetAs<ResolvedLiteral>()->value();
  return !literal.is_null() && expected.Equals(literal);
}

// Returns true if a given expr is a literal and its value >= lower_bound.
//
// Note, that the type of <expr> must be Int64, Uint64 or Numeric,
// otherwise an internal error is returned.
absl::StatusOr<bool> IsLiteralWithValueGreaterThanOrEqualTo(
    const ResolvedExpr& expr, int64_t lower_bound) {
  if (expr.node_kind() != RESOLVED_LITERAL) {
    return false;
  }
  const Value lower = ToIntValueOrInvalid(*expr.type(), lower_bound);
  ZETASQL_RET_CHECK(lower.is_valid());

  const Value& literal = expr.GetAs<ResolvedLiteral>()->value();
  return !literal.is_null() &&
         (lower.LessThan(literal) || lower.Equals(literal));
}

// Returns true if a given expr is a literal and its value is not a NULL.
bool IsNonNullLiteral(const ResolvedExpr& expr) {
  if (expr.node_kind() != RESOLVED_LITERAL) {
    return false;
  }
  return !expr.GetAs<ResolvedLiteral>()->value().is_null();
}

// Returns true if a given expr is uid column.
bool IsUidColumn(const ResolvedExpr& expr, int64_t uid_column_id) {
  if (expr.node_kind() != RESOLVED_COLUMN_REF) {
    return false;
  }
  return expr.GetAs<ResolvedColumnRef>()->column().column_id() == uid_column_id;
}

// Returns true if the column (corresponding to a given column_id)
// is a function counting unique users:
// 1. ANON_COUNT(* CLAMPED BETWEEN 0 AND 1)
// 2. ANON_COUNT($X CLAMPED BETWEEN 0 AND 1) where X is non-null literal
// 3. ANON_COUNT(uid CLAMPED BETWEEN 0 AND 1)
// 4. ANON_SUM($X CLAMPED BETWEEN 0 AND 1) where X is non-null literal and X
//    >= 1
// 5. $differential_privacy_count(*, contribution_bounds_per_group => (0,1))
// 6. $differential_privacy_count($X, contribution_bounds_per_group => (0,1))
//    where X is non-null literal.
// 7. $differential_privacy_count(uid, contribution_bounds_per_group => (0,1))
// 8. $differential_privacy_count($X, contribution_bounds_per_group => (0,1))
//    where X is non-null literal and X >= 1.
bool IsCountUniqueUsers(const ResolvedAggregateFunctionCall* function_call,
                        int64_t uid_column_id) {
  const std::vector<std::unique_ptr<const ResolvedExpr>>& arguments =
      function_call->argument_list();
  auto check_dp_contribution_bounds = [](const ResolvedExpr& expr) {
    if (expr.node_kind() != RESOLVED_LITERAL) {
      return false;
    }
    if (!expr.type()->IsStruct() ||
        expr.type()->AsStruct()->num_fields() != 2) {
      return false;
    }
    const Value expected_lower_bound =
        ToIntValueOrInvalid(*expr.type()->AsStruct()->field(0).type, 0);
    const Value expected_upper_bound =
        ToIntValueOrInvalid(*expr.type()->AsStruct()->field(1).type, 1);

    const Value& literal = expr.GetAs<ResolvedLiteral>()->value();
    return !literal.is_null() && literal.num_fields() == 2 &&
           expected_lower_bound.is_valid() &&
           expected_lower_bound.Equals(literal.field(0)) &&
           expected_upper_bound.is_valid() &&
           expected_upper_bound.Equals(literal.field(1));
  };

  switch (function_call->signature().context_id()) {
    // ANON_COUNT(* CLAMPED BETWEEN 0 AND 1)
    case FunctionSignatureId::FN_ANON_COUNT_STAR:
    case FunctionSignatureId::FN_ANON_COUNT_STAR_WITH_REPORT_PROTO:
    case FunctionSignatureId::FN_ANON_COUNT_STAR_WITH_REPORT_JSON:
      return arguments.size() == 2 &&
             // CLAMPED BETWEEN 0 AND 1
             IsLiteralWithValueEqualTo(*arguments[0], /* expected_value=*/0)
                 .value_or(false) &&
             IsLiteralWithValueEqualTo(*arguments[1],
                                       /* expected_value=*/1)
                 .value_or(false);
    // ANON_COUNT($X CLAMPED BETWEEN 0 AND 1), X - non-null literal
    // ANON_COUNT(uid CLAMPED BETWEEN 0 AND 1)
    case FunctionSignatureId::FN_ANON_COUNT:
    case FunctionSignatureId::FN_ANON_COUNT_WITH_REPORT_PROTO:
    case FunctionSignatureId::FN_ANON_COUNT_WITH_REPORT_JSON:
      return arguments.size() == 3 &&
             (IsNonNullLiteral(*arguments[0]) ||
              IsUidColumn(*arguments[0], uid_column_id)) &&
             // CLAMPED BETWEEN 0 AND 1
             IsLiteralWithValueEqualTo(*arguments[1],
                                       /* expected_value=*/0)
                 .value_or(false) &&
             IsLiteralWithValueEqualTo(*arguments[2],
                                       /* expected_value=*/1)
                 .value_or(false);
    // ANON_SUM($X CLAMPED BETWEEN 0 AND 1), X  >= 1
    case FunctionSignatureId::FN_ANON_SUM_INT64:
    case FunctionSignatureId::FN_ANON_SUM_WITH_REPORT_PROTO_INT64:
    case FunctionSignatureId::FN_ANON_SUM_WITH_REPORT_JSON_INT64:
    case FunctionSignatureId::FN_ANON_SUM_UINT64:
    case FunctionSignatureId::FN_ANON_SUM_WITH_REPORT_PROTO_UINT64:
    case FunctionSignatureId::FN_ANON_SUM_WITH_REPORT_JSON_UINT64:
    case FunctionSignatureId::FN_ANON_SUM_NUMERIC:
      return arguments.size() == 3 &&
             IsLiteralWithValueGreaterThanOrEqualTo(*arguments[0],
                                                    /* lower_bound=*/1)
                 .value_or(false) &&
             // CLAMPED BETWEEN 0 AND 1
             IsLiteralWithValueEqualTo(*arguments[1],
                                       /* expected_value=*/0)
                 .value_or(false) &&
             IsLiteralWithValueEqualTo(*arguments[2],
                                       /* expected_value=*/1)
                 .value_or(false);

    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_COUNT:
      return arguments.size() == 2 &&
             (IsNonNullLiteral(*arguments[0]) ||
              IsUidColumn(*arguments[0], uid_column_id)) &&
             check_dp_contribution_bounds(*arguments[1]);

    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_COUNT_STAR:
      return arguments.size() == 1 &&
             check_dp_contribution_bounds(*arguments[0]);

    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_SUM_INT64:
      return arguments.size() == 2 &&
             IsLiteralWithValueGreaterThanOrEqualTo(*arguments[0],
                                                    /* lower_bound=*/1)
                 .value_or(false) &&
             check_dp_contribution_bounds(*arguments[1]);

    // TODO: For new dp syntax we expect group threshold
    // expression to be INT64.
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_SUM_NUMERIC:
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_SUM_UINT64:
    // TODO: implement WITH_REPORT logic in the follow-up CLs.
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_SUM_REPORT_JSON_INT64:
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_SUM_REPORT_JSON_UINT64:
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_SUM_REPORT_PROTO_INT64:
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_SUM_REPORT_PROTO_UINT64:
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_COUNT_REPORT_JSON:
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_COUNT_REPORT_PROTO:
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_COUNT_STAR_REPORT_JSON:
    case FunctionSignatureId::FN_DIFFERENTIAL_PRIVACY_COUNT_STAR_REPORT_PROTO:
      return false;
    default:
      return false;
  }

  return false;
}

// Rewrites the aggregate list for the outer cross-user aggregate scan. Replaces
// each ANON_* function call with a matching ANON_* function call, but pointing
// the first argument to the appropriate intermediate column produced by the
// per-user aggregate scan.
class OuterAggregateListRewriterVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  OuterAggregateListRewriterVisitor(
      const std::map<ResolvedColumn, ResolvedColumn>& injected_col_map,
      Resolver* resolver, ResolvedColumn inner_uid_column)
      : injected_col_map_(injected_col_map),
        resolver_(resolver),
        inner_uid_column_(inner_uid_column) {}

  ResolvedColumn GetUniqueUserCountColumn() {
    return unique_users_count_column_;
  }

  // Rewrite the outer aggregate list, changing each ANON_* function to refer to
  // the intermediate column with pre-aggregated values that was produced by the
  // per-user aggregate scan.
  absl::StatusOr<std::vector<std::unique_ptr<ResolvedComputedColumn>>>
  RewriteAggregateColumns(const ResolvedAggregateScanBase* node) {
    return ProcessNodeList(node->aggregate_list());
  }

 private:
  absl::Status VisitResolvedAggregateFunctionCall(
      const ResolvedAggregateFunctionCall* node) override {
    ZETASQL_ASSIGN_OR_RETURN(std::vector<std::unique_ptr<ResolvedExpr>> argument_list,
                     ProcessNodeList(node->argument_list()));

    // Resolve the new cross-user ANON_* function call.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> result,
                     ResolveOuterAggregateFunctionCallForAnonFunction(
                         node, injected_col_map_.at(current_column_),
                         // This is expecting unique_ptr to be const.
                         // std::vector<std::unique_ptr<__const__ ResolvedExpr>>
                         {std::make_move_iterator(argument_list.begin()),
                          std::make_move_iterator(argument_list.end())},
                         resolver_));
    ZETASQL_RET_CHECK_EQ(result->node_kind(), RESOLVED_AGGREGATE_FUNCTION_CALL)
        << result->DebugString();

    const auto* function_call =
        node->GetAs<const zetasql::ResolvedAggregateFunctionCall>();

    if (resolver_->language().LanguageFeatureEnabled(
            FEATURE_ANONYMIZATION_THRESHOLDING) ||
        resolver_->language().LanguageFeatureEnabled(
            FEATURE_DIFFERENTIAL_PRIVACY_THRESHOLDING)) {
      // Save first found column which matches unique user count function.
      // We choose to select first to make the unit tests deterministic.
      // In general, we can safely select any matching function.
      // Since, ignoring the intrinsic randomness in these functions, we'll get
      // indistinguishable query results regardless of which function we
      // replace.
      if (!unique_users_count_column_.IsInitialized() &&
          IsCountUniqueUsers(function_call, inner_uid_column_.column_id())) {
        unique_users_count_column_ = current_column_;
      }
    }

    PushNodeToStack(std::move(result));
    return absl::OkStatus();
  }

  absl::Status VisitResolvedComputedColumn(
      const ResolvedComputedColumn* node) override {
    // This function is in practice the class entry point. We need to record
    // what the current output column is so that we can look the appropriate
    // intermediate column up in the map.
    current_column_ = node->column();
    return CopyVisitResolvedComputedColumn(node);
  }

  const std::map<ResolvedColumn, ResolvedColumn>& injected_col_map_;
  ResolvedColumn current_column_;
  Resolver* resolver_;
  // This field should be set to a first found user aggregation function
  // which counts the unique users.
  ResolvedColumn unique_users_count_column_;
  const ResolvedColumn inner_uid_column_;
};

// This class is used by VisitResolvedTVFScan to validate that none of the TVF
// argument trees contain nodes that are not supported yet as TVF arguments.
//
// The current implementation does not support subqueries that have
// anonymization, where we will need to recursively call the rewriter on
// these (sub)queries.
class TVFArgumentValidatorVisitor : public ResolvedASTVisitor {
 public:
  explicit TVFArgumentValidatorVisitor(const std::string& tvf_name)
      : tvf_name_(tvf_name) {}

  absl::Status VisitResolvedAnonymizedAggregateScan(
      const ResolvedAnonymizedAggregateScan* node) override {
    return MakeSqlErrorAtNode(*node)
           << "TVF arguments do not support SELECT WITH ANONYMIZATION queries";
  }

  absl::Status VisitResolvedDifferentialPrivacyAggregateScan(
      const ResolvedDifferentialPrivacyAggregateScan* node) override {
    return MakeSqlErrorAtNode(*node) << "TVF arguments do not support SELECT "
                                        "WITH DIFFERENTIAL_PRIVACY queries";
  }

  absl::Status VisitResolvedProjectScan(
      const ResolvedProjectScan* node) override {
    return MaybeAttachParseLocation(
        ResolvedASTVisitor::VisitResolvedProjectScan(node), *node);
  }

 private:
  const std::string tvf_name_;
};

std::string FieldPathExpressionToString(const ResolvedExpr* expr) {
  std::vector<std::string> field_path;
  while (expr != nullptr) {
    switch (expr->node_kind()) {
      case RESOLVED_GET_PROTO_FIELD: {
        auto* node = expr->GetAs<ResolvedGetProtoField>();
        field_path.emplace_back(node->field_descriptor()->name());
        expr = node->expr();
        break;
      }
      case RESOLVED_GET_STRUCT_FIELD: {
        auto* node = expr->GetAs<ResolvedGetStructField>();
        field_path.emplace_back(
            node->expr()->type()->AsStruct()->field(node->field_idx()).name);
        expr = node->expr();
        break;
      }
      case RESOLVED_COLUMN_REF: {
        std::string name = expr->GetAs<ResolvedColumnRef>()->column().name();
        if (!IsInternalAlias(name)) {
          field_path.emplace_back(std::move(name));
        }
        expr = nullptr;
        break;
      }
      default:
        // Node types other than RESOLVED_GET_PROTO_FIELD /
        // RESOLVED_GET_STRUCT_FIELD / RESOLVED_COLUMN_REF should never show up
        // in a $uid column path expression.
        return "<INVALID>";
    }
  }
  return absl::StrJoin(field_path.rbegin(), field_path.rend(), ".");
}

// Wraps the ResolvedColumn for a given $uid column during AST rewrite. Also
// tracks an optional alias for the column, this improves error messages with
// aliased tables.
struct UidColumnState {
  void InitFromValueTable(const ResolvedComputedColumn* projected_userid_column,
                          std::string value_table_alias) {
    column = projected_userid_column->column();
    alias = std::move(value_table_alias);
    value_table_uid = projected_userid_column->expr();
  }

  void Clear() {
    column.Clear();
    alias.clear();
    value_table_uid = nullptr;
  }

  bool SetColumn(const zetasql::ResolvedColumn& col) {
    column = col;
    return true;
  }

  bool SetColumn(const zetasql::ResolvedColumn& col,
                 const std::string& new_alias) {
    SetColumn(col);
    alias = new_alias;
    return true;
  }

  // Returns an alias qualified (if specified) user visible name for the $uid
  // column to be returned in validation error messages.
  std::string ToString() const {
    const std::string alias_prefix =
        absl::StrCat(alias.empty() ? "" : absl::StrCat(alias, "."));
    if (!IsInternalAlias(column.name())) {
      return absl::StrCat(alias_prefix, column.name());
    } else if (value_table_uid != nullptr) {
      return absl::StrCat(alias_prefix,
                          FieldPathExpressionToString(value_table_uid));
    } else {
      return "";
    }
  }

  // If the uid column is derived from a value table we insert a
  // ResolvedProjectScan that extracts the uid column from the table row object.
  // But existing references to the uid column in the query (like in a group by
  // list) will reference a semantically equivalent but distinct column. This
  // function replaces these semantically equivalent computed columns with
  // column references to the 'canonical' uid column.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
  SubstituteUidComputedColumn(
      std::vector<std::unique_ptr<const ResolvedComputedColumn>> expr_list) {
    if (value_table_uid == nullptr) return expr_list;
    for (auto& col : expr_list) {
      if (MatchesPathExpression(*col->expr())) {
        col = MakeResolvedComputedColumn(col->column(), MakeColRef(column));
        column = col->column();
        value_table_uid = nullptr;
      }
    }

    return expr_list;
  }

  // Add the $uid column to the argument scan node column list if it isn't
  // already included.
  void ProjectIfMissing(ResolvedScan& node) {
    for (const ResolvedColumn& col : node.column_list()) {
      if (col == column) {
        return;
      }
    }
    node.add_column_list(column);
  }

  // Returns true IFF the argument expression points to the same (optionally
  // nested) value as this.
  bool MatchesPathExpression(const ResolvedExpr& other) const {
    if (value_table_uid == nullptr) {
      if (other.node_kind() == RESOLVED_COLUMN_REF) {
        return other.GetAs<ResolvedColumnRef>()->column() == column;
      }
      return false;
    }
    return IsSameFieldPath(&other, value_table_uid,
                           FieldPathMatchingOption::kExpression);
  }

  // A column declared as the $uid column in a table or TVF schema definition.
  // This gets passed up the AST during the rewriting process to validate the
  // query, and gets replaced with computed columns as needed for joins and
  // nested aggregations.
  ResolvedColumn column;

  // <alias> is only used for clarifying error messages, it's only set to a non
  // empty string for table scan clauses like '... FROM Table as t' so that we
  // can display error messages related to the $uid column as 't.userid' rather
  // than 'userid' or 'Table.userid'. It has no impact on the actual rewriting
  // logic.
  std::string alias;

 private:
  const ResolvedExpr* value_table_uid = nullptr;
};

// Tracks the lazily-rewritten state of a ResolvedWithEntry. The original AST
// must outlive instances of this struct.
struct WithEntryRewriteState {
  // References the WITH entry in the original AST, always set.
  const ResolvedWithEntry& original_entry;

  // Contains the rewritten AST for this WITH entry, but only if it's been
  // rewritten.
  const ResolvedWithEntry* rewritten_entry;
  std::unique_ptr<const ResolvedWithEntry> rewritten_entry_owned;

  // Contains the $uid column state for this WITH entry IFF it's been rewritten
  // AND it reads from a table, TVF, or another WITH entry that reads user data.
  std::optional<UidColumnState> rewritten_uid;
};

// A helper for JoinExprIncludesUid, returns true if at least one argument of
// the function call is a column ref referring to left_uid, and the same for
// right_uid.
bool FunctionReferencesUid(const ResolvedFunctionCall* call,
                           const UidColumnState& left_uid,
                           const UidColumnState& right_uid) {
  bool left_referenced = false;
  bool right_referenced = false;
  for (const std::unique_ptr<const ResolvedExpr>& argument :
       call->argument_list()) {
    left_referenced |= left_uid.MatchesPathExpression(*argument);
    right_referenced |= right_uid.MatchesPathExpression(*argument);
  }
  return left_referenced && right_referenced;
}

// A helper function for checking if a join expression between two tables
// containing user data meets our requirements for joining on the $uid column in
// each table.
//
// Returns true IFF join_expr contains a top level AND function, or an AND
// function nested inside another AND function (arbitrarily deep), that contains
// an EQUAL function that satisfies FunctionReferencesUid.
//
// This excludes a number of logically equivalent join expressions
// (e.g. !(left != right)), but that's fine, we want queries to be intentional.
bool JoinExprIncludesUid(const ResolvedExpr* join_expr,
                         const UidColumnState& left_uid,
                         const UidColumnState& right_uid) {
  if (join_expr->node_kind() != RESOLVED_FUNCTION_CALL) {
    return false;
  }
  const ResolvedFunctionCall* call = join_expr->GetAs<ResolvedFunctionCall>();
  const Function* function = call->function();
  if (!function->IsScalar() || !function->IsZetaSQLBuiltin()) {
    return false;
  }
  switch (call->signature().context_id()) {
    case FN_AND:
      for (const std::unique_ptr<const ResolvedExpr>& argument :
           call->argument_list()) {
        if (JoinExprIncludesUid(argument.get(), left_uid, right_uid)) {
          return true;
        }
      }
      break;
    case FN_EQUAL:
      if (FunctionReferencesUid(call, left_uid, right_uid)) {
        return true;
      }
      break;
  }
  return false;
}

constexpr absl::string_view SetOperationTypeToString(
    const ResolvedSetOperationScanEnums::SetOperationType type) {
  switch (type) {
    case ResolvedSetOperationScanEnums::UNION_ALL:
      return "UNION ALL";
    case ResolvedSetOperationScanEnums::UNION_DISTINCT:
      return "UNION DISTINCT";
    case ResolvedSetOperationScanEnums::INTERSECT_ALL:
      return "INTERSECT ALL";
    case ResolvedSetOperationScanEnums::INTERSECT_DISTINCT:
      return "INTERSECT DISTINCT";
    case ResolvedSetOperationScanEnums::EXCEPT_ALL:
      return "EXCEPT ALL";
    case ResolvedSetOperationScanEnums::EXCEPT_DISTINCT:
      return "EXCEPT DISTINCT";
  }
}

// Used to validate expression subqueries visited by PerUserRewriterVisitor.
// Rejects nested anonymization operations and reads of user data based on
// (broken link).
class ExpressionSubqueryRewriterVisitor : public ResolvedASTDeepCopyVisitor {
  absl::Status VisitResolvedTableScan(const ResolvedTableScan* node) override {
    if (node->table()->SupportsAnonymization()) {
      return MakeSqlErrorAtNode(*node)
             << "Reading the table " << node->table()->Name()
             << " containing user data in expression subqueries is not allowed";
    }
    return CopyVisitResolvedTableScan(node);
  }

  absl::Status VisitResolvedTVFScan(const ResolvedTVFScan* node) override {
    if (node->signature()->SupportsAnonymization()) {
      return MakeSqlErrorAtNode(*node)
             << "Reading the TVF " << node->tvf()->FullName()
             << " containing user data in expression subqueries is not allowed";
    }
    return CopyVisitResolvedTVFScan(node);
  }

  absl::Status VisitResolvedAnonymizedAggregateScan(
      const ResolvedAnonymizedAggregateScan* node) override {
    return MakeSqlErrorAtNode(*node)
           << "Nested anonymization query is not implemented yet";
  }

  absl::Status VisitResolvedProjectScan(
      const ResolvedProjectScan* node) override {
    // Necessary to correctly attach parse location to errors generated above.
    return MaybeAttachParseLocation(CopyVisitResolvedProjectScan(node), *node);
  }
};

// Rewrites the rest of the per-user scan, propagating the AnonymizationInfo()
// userid (aka $uid column) from the base private table scan to the top node
// returned.
//
// This visitor may only be invoked on a scan that is a transitive child of a
// ResolvedAnonymizedAggregateScan. uid_column() will return an error if the
// subtree represented by that scan does not contain a table or TVF that
// contains user data (AnonymizationInfo).
class PerUserRewriterVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  explicit PerUserRewriterVisitor(
      ColumnFactory* allocator, TypeFactory* type_factory, Resolver* resolver,
      std::vector<const ResolvedTableScan*>& resolved_table_scans,
      std::vector<std::unique_ptr<WithEntryRewriteState>>& with_entries,
      SelectWithModeName select_with_mode_name)
      : allocator_(allocator),
        type_factory_(type_factory),
        resolver_(resolver),
        resolved_table_scans_(resolved_table_scans),
        with_entries_(with_entries),
        select_with_mode_name_(select_with_mode_name) {}

  std::optional<ResolvedColumn> uid_column() const {
    if (current_uid_.column.IsInitialized()) {
      return current_uid_.column;
    } else {
      return std::nullopt;
    }
  }

  const UidColumnState& uid_column_state() const { return current_uid_; }

 private:
  absl::Status ProjectValueTableScanRowValueIfNeeded(
      ResolvedTableScan* copy, const Column* value_table_value_column,
      ResolvedColumn* value_table_value_resolved_column) {
    for (int i = 0; i < copy->column_list_size(); ++i) {
      int j = copy->column_index_list(i);
      if (value_table_value_column == copy->table()->GetColumn(j)) {
        // The current scan already produces the value table value column
        // that we want to extract from, so we can leave the scan node
        // as is.
        *value_table_value_resolved_column = copy->column_list(i);
        return absl::OkStatus();
      }
    }

    // Make a new ResolvedColumn for the value table value column and
    // add it to the table scan's column list.
    *value_table_value_resolved_column = allocator_->MakeCol(
        "$table_scan", "$value", value_table_value_column->GetType());
    copy->add_column_list(*value_table_value_resolved_column);
    int table_col_idx = -1;
    for (int idx = 0; idx < copy->table()->NumColumns(); ++idx) {
      if (value_table_value_column == copy->table()->GetColumn(idx)) {
        table_col_idx = idx;
        break;
      }
    }
    ZETASQL_RET_CHECK_GE(table_col_idx, 0);
    ZETASQL_RET_CHECK_LT(table_col_idx, copy->table()->NumColumns());
    copy->add_column_index_list(table_col_idx);

    return absl::OkStatus();
  }

  absl::StatusOr<std::unique_ptr<ResolvedComputedColumn>>
  MakeGetFieldComputedColumn(
      const ResolvedScan* node,
      absl::Span<const std::string> userid_column_name_path,
      const ResolvedColumn& value_table_value_resolved_column) {
    const std::string& userid_column_name =
        IdentifierPathToString(userid_column_name_path);
    ResolvedColumn userid_column = value_table_value_resolved_column;
    std::unique_ptr<const ResolvedExpr> resolved_expr_to_ref =
        MakeColRef(value_table_value_resolved_column);

    if (value_table_value_resolved_column.type()->IsStruct()) {
      const StructType* struct_type =
          value_table_value_resolved_column.type()->AsStruct();

      for (const std::string& userid_column_field : userid_column_name_path) {
        ZETASQL_RET_CHECK_NE(struct_type, nullptr) << userid_column_name;
        int found_idx = -1;
        bool is_ambiguous = false;
        const StructField* struct_field = struct_type->FindField(
            userid_column_field, &is_ambiguous, &found_idx);
        ZETASQL_RET_CHECK_NE(struct_field, nullptr) << userid_column_name;
        ZETASQL_RET_CHECK(!is_ambiguous) << userid_column_name;
        struct_type = struct_field->type->AsStruct();

        std::unique_ptr<ResolvedExpr> get_userid_field_expr =
            MakeResolvedGetStructField(
                struct_field->type, std::move(resolved_expr_to_ref), found_idx);

        userid_column = allocator_->MakeCol(
            "$project", absl::StrCat("$", userid_column_field),
            get_userid_field_expr->type());
        resolved_expr_to_ref = std::move(get_userid_field_expr);
      }

    } else {
      const google::protobuf::Descriptor* descriptor =
          value_table_value_resolved_column.type()->AsProto()->descriptor();

      for (const std::string& userid_column_field : userid_column_name_path) {
        ZETASQL_RET_CHECK_NE(descriptor, nullptr) << userid_column_name;
        const google::protobuf::FieldDescriptor* field =
            ProtoType::FindFieldByNameIgnoreCase(descriptor,
                                                 userid_column_field);
        if (field == nullptr) {
          return MakeSqlErrorAtNode(*node)
                 << "Unable to find "
                 << absl::AsciiStrToLower(select_with_mode_name_.name)
                 << " user ID column " << userid_column_name
                 << " in value table fields";
        }
        descriptor = field->message_type();

        const Type* field_type;
        ZETASQL_RETURN_IF_ERROR(type_factory_->GetProtoFieldType(
            field,
            value_table_value_resolved_column.type()
                ->AsProto()
                ->CatalogNamePath(),
            &field_type));

        Value default_value;
        ZETASQL_RETURN_IF_ERROR(
            GetProtoFieldDefault(ProtoFieldDefaultOptions::FromFieldAndLanguage(
                                     field, resolver_->language()),
                                 field, field_type, &default_value));

        // Note that we use 'return_default_value_when_unset' as false here
        // because it indicates behavior for when the parent message is unset,
        // not when the extracted field is unset (whose behavior depends on the
        // field annotations, e.g., use_field_defaults).
        std::unique_ptr<ResolvedExpr> get_userid_field_expr =
            MakeResolvedGetProtoField(
                field_type, std::move(resolved_expr_to_ref), field,
                default_value,
                /*get_has_bit=*/false, ProtoType::GetFormatAnnotation(field),
                /*return_default_value_when_unset=*/false);
        userid_column = allocator_->MakeCol(
            "$project", absl::StrCat("$", userid_column_field),
            get_userid_field_expr->type());

        resolved_expr_to_ref = std::move(get_userid_field_expr);
      }
    }
    return MakeResolvedComputedColumn(userid_column,
                                      std::move(resolved_expr_to_ref));
  }

  absl::Status VisitResolvedTableScan(const ResolvedTableScan* node) override {
    ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedTableScan(node));
    ResolvedTableScan* copy = GetUnownedTopOfStack<ResolvedTableScan>();

    if (!copy->table()->SupportsAnonymization()) {
      return absl::OkStatus();
    }

    // There exists an authoritative $uid column in the underlying table.
    //
    // For value tables, the Column itself doesn't exist in the table,
    // but its Column Name identifies the $uid field name of the value table
    // Value.
    ZETASQL_RET_CHECK(copy->table()->GetAnonymizationInfo().has_value());
    // Save the table alias with the $uid column. If the table doesn't have an
    // alias, copy->alias() returns an empty string and the $uid column alias
    // gets cleared.
    current_uid_.alias = copy->alias();
    const Column* table_col = copy->table()
                                  ->GetAnonymizationInfo()
                                  .value()
                                  .GetUserIdInfo()
                                  .get_column();
    resolved_table_scans_.push_back(copy);
    if (table_col != nullptr) {
      // The userid column is an actual physical column from the table, so
      // find it and make sure it's part of the table's output column list.
      //
      // For each ResolvedColumn column_list[i], the matching table column is
      // table->GetColumn(column_index_list[i])
      for (int i = 0; i < copy->column_list_size(); ++i) {
        int j = copy->column_index_list(i);
        if (table_col == copy->table()->GetColumn(j)) {
          // If the original query selects the $uid column, reuse it.
          current_uid_.SetColumn(copy->column_list(i));
          ZETASQL_RETURN_IF_ERROR(ValidateUidColumnSupportsGrouping(*node));
          return absl::OkStatus();
        }
      }

      if (current_uid_.SetColumn(allocator_->MakeCol(copy->table()->Name(),
                                                     table_col->Name(),
                                                     table_col->GetType()))) {
        copy->add_column_list(current_uid_.column);

        int table_col_id = -1;
        for (int i = 0; i < copy->table()->NumColumns(); ++i) {
          if (table_col == copy->table()->GetColumn(i)) {
            table_col_id = i;
          }
        }
        ZETASQL_RET_CHECK_NE(table_col_id, -1);
        copy->add_column_index_list(table_col_id);
      }
    } else {
      // The userid column is identified by the column name.  This case
      // happens when the table is a value table, and the userid column is
      // derived from the value table's value.
      //
      // In this case, the $uid column is derived by fetching the
      // proper struct/proto field from the table value type.  We create
      // a new Project node on top of the input scan node that projects
      // all of the scan columns, along with one new column that is the
      // GetProto/StructField expression to extract the userid column.

      // First, ensure that the Table's row value is projected from the scan
      // (it may not be projected, for instance, if the full original query
      // is just ANON_COUNT(*)).
      //
      // As per the Table contract, value tables require their first column
      // (column 0) to be the value table value column.
      ZETASQL_RET_CHECK_GE(copy->table()->NumColumns(), 1);
      const Column* value_table_value_column = copy->table()->GetColumn(0);
      ZETASQL_RET_CHECK_NE(value_table_value_column, nullptr) << copy->table()->Name();
      ZETASQL_RET_CHECK(value_table_value_column->GetType()->IsStruct() ||
                value_table_value_column->GetType()->IsProto());

      ResolvedColumn value_table_value_resolved_column;
      ZETASQL_RETURN_IF_ERROR(ProjectValueTableScanRowValueIfNeeded(
          copy, value_table_value_column, &value_table_value_resolved_column));

      ZETASQL_RET_CHECK(value_table_value_resolved_column.IsInitialized())
          << value_table_value_resolved_column.DebugString();

      // Build an expression to extract the userid column from the
      // value table row value.
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<ResolvedComputedColumn> projected_userid_column,
          MakeGetFieldComputedColumn(node,
                                     copy->table()
                                         ->GetAnonymizationInfo()
                                         .value()
                                         .UserIdColumnNamePath(),
                                     value_table_value_resolved_column));

      current_uid_.InitFromValueTable(projected_userid_column.get(),
                                      copy->alias());

      // Create a new Project node that projects the extracted userid
      // field from the table's row (proto or struct) value.
      std::vector<ResolvedColumn> project_column_list_with_userid =
          copy->column_list();
      project_column_list_with_userid.emplace_back(current_uid_.column);

      PushNodeToStack(MakeResolvedProjectScan(
          project_column_list_with_userid,
          MakeNodeVector(std::move(projected_userid_column)),
          ConsumeTopOfStack<ResolvedScan>()));
    }
    ZETASQL_RETURN_IF_ERROR(ValidateUidColumnSupportsGrouping(*node));
    return absl::OkStatus();
  }

  absl::Status VisitResolvedTVFScan(const ResolvedTVFScan* node) override {
    // We do not currently allow TVF arguments to contain anonymization,
    // because we are not invoking the rewriter on the TVF arguments yet.
    for (const std::unique_ptr<const ResolvedFunctionArgument>& arg :
         node->argument_list()) {
      TVFArgumentValidatorVisitor visitor(node->tvf()->FullName());
      ZETASQL_RETURN_IF_ERROR(arg->Accept(&visitor));
    }

    {
      ResolvedASTDeepCopyVisitor copy_visitor;
      ZETASQL_RETURN_IF_ERROR(node->Accept(&copy_visitor));
      ZETASQL_ASSIGN_OR_RETURN(auto copy,
                       copy_visitor.ConsumeRootNode<ResolvedTVFScan>());
      PushNodeToStack(std::move(copy));
    }
    ResolvedTVFScan* copy = GetUnownedTopOfStack<ResolvedTVFScan>();

    // The TVF doesn't produce user data or an anonymization userid column, so
    // we can return early.
    //
    // TODO: Figure out how we can take an early exit without the
    // copy. Does this method take ownership of <node>? Can we effectively
    // push it back onto the top of the stack (which requires a non-const
    // std::unique_ptr<ResolvedNode>)?  I tried creating a non-const unique_ptr
    // and that failed with what looked like a double free condition.  It's
    // unclear at present what the contracts are and how we can avoid the
    // needless copy.
    if (!copy->signature()->SupportsAnonymization()) {
      return absl::OkStatus();
    }

    if (copy->signature()->result_schema().is_value_table()) {
      ZETASQL_RET_CHECK_EQ(copy->signature()->result_schema().num_columns(), 1);
      const std::optional<const AnonymizationInfo> anonymization_info =
          copy->signature()->GetAnonymizationInfo();
      ZETASQL_RET_CHECK(anonymization_info.has_value());

      ResolvedColumn value_column;
      // Check if the value table column is already being projected.
      if (copy->column_list_size() > 0) {
        ZETASQL_RET_CHECK_EQ(copy->column_list_size(), 1);
        value_column = copy->column_list(0);
      } else {
        // Create and project the column of the entire proto.
        value_column = allocator_->MakeCol(
            copy->tvf()->Name(), "$value",
            copy->signature()->result_schema().column(0).type);
        copy->mutable_column_list()->push_back(value_column);
        copy->mutable_column_index_list()->push_back(0);
      }

      // Build an expression to extract the userid column from the
      // value table row value.
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<ResolvedComputedColumn> projected_userid_column,
          MakeGetFieldComputedColumn(
              node, anonymization_info->UserIdColumnNamePath(), value_column));

      current_uid_.InitFromValueTable(projected_userid_column.get(),
                                      copy->alias());

      std::vector<ResolvedColumn> project_column_list_with_userid =
          copy->column_list();
      project_column_list_with_userid.emplace_back(current_uid_.column);

      PushNodeToStack(MakeResolvedProjectScan(
          project_column_list_with_userid,
          MakeNodeVector(std::move(projected_userid_column)),
          ConsumeTopOfStack<ResolvedScan>()));

      ZETASQL_RETURN_IF_ERROR(ValidateUidColumnSupportsGrouping(*node));
      return absl::OkStatus();
    }

    if (copy->signature()
            ->GetAnonymizationInfo()
            ->UserIdColumnNamePath()
            .size() > 1) {
      return MakeSqlErrorAtNode(*node)
             << "Nested user IDs are not currently supported for TVFs (in TVF "
             << copy->tvf()->FullName() << ")";
    }
    // Since we got to here, the TVF produces a userid column so we must ensure
    // that the column is projected for use in the anonymized aggregation.
    const std::string& userid_column_name = copy->signature()
                                                ->GetAnonymizationInfo()
                                                ->GetUserIdInfo()
                                                .get_column_name();

    // Check if the $uid column is already being projected.
    for (int i = 0; i < copy->column_list_size(); ++i) {
      // Look up the schema column name in the index list.
      const std::string& result_column_name =
          copy->signature()
              ->result_schema()
              .column(copy->column_index_list(i))
              .name;
      if (result_column_name == userid_column_name) {
        // Already projected, we're done.
        current_uid_.SetColumn(copy->column_list(i), copy->alias());
        return absl::OkStatus();
      }
    }

    // We need to project the $uid column. Look it up by name in the TVF schema
    // to get type information and record it in column_index_list.
    int tvf_userid_column_index = -1;
    for (int i = 0; i < copy->signature()->result_schema().num_columns(); ++i) {
      if (userid_column_name ==
          copy->signature()->result_schema().column(i).name) {
        tvf_userid_column_index = i;
        break;
      }
    }
    // Engines should normally validate the userid column when creating/adding
    // the TVF to the catalog whenever possible. However, this is not possible
    // in all cases - for example for templated TVFs where the output schema is
    // unknown until call time. So we produce a user-facing error message in
    // this case.
    if (tvf_userid_column_index == -1) {
      return MakeSqlErrorAtNode(*node)
             << "The " << absl::AsciiStrToLower(select_with_mode_name_.name)
             << " userid column " << userid_column_name << " defined for TVF "
             << copy->tvf()->FullName()
             << " was not found in the output schema of the TVF";
    }

    // Create and project the new $uid column.
    ResolvedColumn uid_column =
        allocator_->MakeCol(copy->tvf()->Name(), userid_column_name,
                            copy->signature()
                                ->result_schema()
                                .column(tvf_userid_column_index)
                                .type);

    // Per the ResolvedTVFScan contract:
    //   <column_list> is a set of ResolvedColumns created by this scan.
    //   These output columns match positionally with the columns in the output
    //   schema of <signature>
    // To satisfy this contract we must also insert the $uid column
    // positionally. The target location is at the first value in
    // column_index_list that is greater than tvf_userid_column_index (because
    // it is positional the indices must be ordered).
    int userid_column_insertion_index = 0;
    for (int i = 0; i < copy->column_index_list_size(); ++i) {
      if (copy->column_index_list(i) > tvf_userid_column_index) {
        userid_column_insertion_index = i;
        break;
      }
    }

    copy->mutable_column_list()->insert(
        copy->column_list().begin() + userid_column_insertion_index,
        uid_column);
    copy->mutable_column_index_list()->insert(
        copy->column_index_list().begin() + userid_column_insertion_index,
        tvf_userid_column_index);
    current_uid_.SetColumn(uid_column, copy->alias());

    return absl::OkStatus();
  }

  absl::Status VisitResolvedWithRefScan(
      const ResolvedWithRefScan* node) override {
    // No $uid column should have been encountered before now
    ZETASQL_RET_CHECK(!current_uid_.column.IsInitialized());

    // Lookup the referenced WITH entry
    auto it = std::find_if(
        with_entries_.begin(), with_entries_.end(),
        [node](const std::unique_ptr<WithEntryRewriteState>& entry) {
          return node->with_query_name() ==
                 entry->original_entry.with_query_name();
        });
    ZETASQL_RET_CHECK(it != with_entries_.end())
        << "Failed to find WITH entry " << node->with_query_name();
    WithEntryRewriteState& entry = **it;

    if (entry.rewritten_entry == nullptr) {
      // This entry hasn't been rewritten yet, rewrite it as if it was just a
      // nested subquery.
      ZETASQL_ASSIGN_OR_RETURN(entry.rewritten_entry_owned,
                       ProcessNode(&entry.original_entry));
      // VisitResolvedWithEntry sets 'entry.rewritten_entry'
      ZETASQL_RET_CHECK_EQ(entry.rewritten_entry, entry.rewritten_entry_owned.get())
          << "Invalid rewrite state for " << node->with_query_name();
    }

    ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedWithRefScan(node));
    if (entry.rewritten_uid && entry.rewritten_uid->column.IsInitialized()) {
      // The WITH entry contained a reference to user data, use its $uid column.
      auto* copy = GetUnownedTopOfStack<ResolvedWithRefScan>();
      // Update $uid column reference. The column_list in the
      // ResolvedWithRefScan matches positionally with the column_list in the
      // ResolvedWithEntry. But if the WithEntry explicitly selects columns and
      // does not include the $uid column, ResolvedWithRefScan will have one
      // less column.
      for (int i = 0;
           i < entry.rewritten_entry->with_subquery()->column_list().size() &&
           i < copy->column_list().size();
           ++i) {
        if (entry.rewritten_entry->with_subquery()
                ->column_list(i)
                .column_id() == entry.rewritten_uid->column.column_id()) {
          current_uid_.SetColumn(copy->column_list(i), "");
          return absl::OkStatus();
        }
      }
    }
    return absl::OkStatus();
  }
  absl::Status VisitResolvedWithEntry(const ResolvedWithEntry* node) override {
    // No $uid column should have been encountered before now
    ZETASQL_RET_CHECK(!current_uid_.column.IsInitialized());
    ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedWithEntry(node));
    // Check if this entry is recorded in 'with_entries_', record the rewritten
    // result and $uid column if so.
    for (auto& entry : with_entries_) {
      if (node->with_query_name() == entry->original_entry.with_query_name()) {
        ZETASQL_RET_CHECK(entry->rewritten_entry == nullptr)
            << "WITH entry has already been rewritten: "
            << node->with_query_name();
        entry->rewritten_entry = GetUnownedTopOfStack<ResolvedWithEntry>();
        entry->rewritten_uid = std::move(current_uid_);
        current_uid_.Clear();
        return absl::OkStatus();
      }
    }
    // Record this entry and corresponding rewrite state for use by
    // VisitResolvedWithRefScan.
    with_entries_.emplace_back(new WithEntryRewriteState{
        .original_entry = *node,
        .rewritten_entry = GetUnownedTopOfStack<ResolvedWithEntry>(),
        .rewritten_uid = std::move(current_uid_)});
    current_uid_.Clear();
    return absl::OkStatus();
  }

  absl::Status VisitResolvedJoinScan(const ResolvedJoinScan* node) override {
    // No $uid column should have been encountered before now
    ZETASQL_RET_CHECK(!current_uid_.column.IsInitialized());

    // Make a simple copy of the join node that we can swap the left and right
    // scans out of later.
    ResolvedASTDeepCopyVisitor join_visitor;
    ZETASQL_RETURN_IF_ERROR(node->Accept(&join_visitor));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedJoinScan> owned_copy,
                     join_visitor.ConsumeRootNode<ResolvedJoinScan>());
    PushNodeToStack(std::move(owned_copy));
    ResolvedJoinScan* copy = GetUnownedTopOfStack<ResolvedJoinScan>();

    // Rewrite and copy the left scan.
    PerUserRewriterVisitor left_visitor(allocator_, type_factory_, resolver_,
                                        resolved_table_scans_, with_entries_,
                                        select_with_mode_name_);
    ZETASQL_RETURN_IF_ERROR(node->left_scan()->Accept(&left_visitor));
    const ResolvedColumn& left_uid = left_visitor.current_uid_.column;
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedScan> left_scan,
                     left_visitor.ConsumeRootNode<ResolvedScan>());
    copy->set_left_scan(std::move(left_scan));

    // Rewrite and copy the right scan.
    PerUserRewriterVisitor right_visitor(allocator_, type_factory_, resolver_,
                                         resolved_table_scans_, with_entries_,
                                         select_with_mode_name_);
    ZETASQL_RETURN_IF_ERROR(node->right_scan()->Accept(&right_visitor));
    const ResolvedColumn& right_uid = right_visitor.current_uid_.column;
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedScan> right_scan,
                     right_visitor.ConsumeRootNode<ResolvedScan>());
    copy->set_right_scan(std::move(right_scan));

    if (!left_uid.IsInitialized() && !right_uid.IsInitialized()) {
      // Two non-private tables
      // Nothing needs to be done
      return absl::OkStatus();
    } else if (left_uid.IsInitialized() && right_uid.IsInitialized()) {
      // Two private tables
      // Both tables have a $uid column, so we add AND Left.$uid = Right.$uid
      // to the join clause after checking that the types are equal and
      // comparable
      // TODO: Revisit if we want to allow $uid type coercion
      if (!left_uid.type()->Equals(right_uid.type())) {
        return MakeSqlErrorAtNode(*copy) << absl::StrCat(
                   "Joining two tables containing private data requires "
                   "matching user id column types, instead got ",
                   Type::TypeKindToString(left_uid.type()->kind(),
                                          resolver_->language().product_mode()),
                   " and ",
                   Type::TypeKindToString(
                       right_uid.type()->kind(),
                       resolver_->language().product_mode()));
      }
      if (!left_uid.type()->SupportsEquality(resolver_->language())) {
        return MakeSqlErrorAtNode(*copy) << absl::StrCat(
                   "Joining two tables containing private data requires "
                   "the user id column types to support equality comparison, "
                   "instead got ",
                   Type::TypeKindToString(
                       left_uid.type()->kind(),
                       resolver_->language().product_mode()));
      }

      // Reject joins with either missing join expressions, or join
      // expressions that don't join on $uid
      // TODO: also support uid constraints with a WHERE clause,
      // for example this query:
      //   select anon_count(*)
      //   from t1, t2
      //   where t1.uid = t2.uid;
      if (copy->join_expr() == nullptr) {
        return MakeSqlErrorAtNode(*copy) << absl::StrCat(
                   "Joins between tables containing private data must "
                   "explicitly join on the user id column in each table",
                   FormatJoinUidError(", add 'ON %s=%s'",
                                      left_visitor.current_uid_,
                                      right_visitor.current_uid_));
      }
      if (!JoinExprIncludesUid(copy->join_expr(), left_visitor.current_uid_,
                               right_visitor.current_uid_)) {
        return MakeSqlErrorAtNode(*copy->join_expr()) << absl::StrCat(
                   "Joins between tables containing private data must also "
                   "explicitly join on the user id column in each table",
                   FormatJoinUidError(
                       ", add 'AND %s=%s' to the join ON expression",
                       left_visitor.current_uid_, right_visitor.current_uid_));
      }
    }

    // At this point, we are either joining two private tables and Left.$uid
    // and Right.$uid are both valid, or joining a private table against a
    // non-private table and exactly one of {Left.$uid, Right.$uid} are valid.
    //
    // Now we want to check if a valid $uid column is being projected, and add
    // an appropriate one based on the join type if not.
    // INNER JOIN: project either Left.$uid or Right.$uid
    // LEFT JOIN:  project (and require) Left.$uid
    // RIGHT JOIN: project (and require) Right.$uid
    // FULL JOIN:  require Left.$uid and Right.$uid, project
    //             COALESCE(Left.$uid, Right.$uid)
    current_uid_.column.Clear();

    switch (node->join_type()) {
      case ResolvedJoinScan::INNER:
        // If both join inputs have a $uid column then project the $uid from
        // the left.  Otherwise project the $uid column from the join input
        // that contains it.
        current_uid_ = (left_uid.IsInitialized() ? left_visitor.current_uid_
                                                 : right_visitor.current_uid_);
        current_uid_.ProjectIfMissing(*copy);
        return absl::OkStatus();

      case ResolvedJoinScan::LEFT:
        // We must project the $uid from the Left table in a left outer join,
        // otherwise we end up with rows with NULL $uid.
        if (!left_uid.IsInitialized()) {
          return MakeSqlErrorAtNode(*copy->left_scan())
                 << "The left table in a LEFT OUTER join must contain user "
                    "data";
        }
        current_uid_ = left_visitor.current_uid_;
        current_uid_.ProjectIfMissing(*copy);
        return absl::OkStatus();

      case ResolvedJoinScan::RIGHT:
        // We must project the $uid from the Right table in a right outer
        // join, otherwise we end up with rows with NULL $uid.
        if (!right_uid.IsInitialized()) {
          return MakeSqlErrorAtNode(*copy->right_scan())
                 << "The right table in a RIGHT OUTER join must contain user "
                    "data";
        }
        current_uid_ = right_visitor.current_uid_;
        current_uid_.ProjectIfMissing(*copy);
        return absl::OkStatus();

      case ResolvedJoinScan::FULL:
        // Full outer joins require both tables to have an attached $uid. We
        // project COALESCE(Left.$uid, Right.$uid) because up to one of the
        // $uid columns may be null for each output row.
        if (!left_uid.IsInitialized() || !right_uid.IsInitialized()) {
          return MakeSqlErrorAtNode(left_uid.IsInitialized()
                                        ? *copy->right_scan()
                                        : *copy->left_scan())
                 << "Both tables in a FULL OUTER join must contain user "
                    "data";
        }

        // Full outer join, the result $uid column is
        // COALESCE(Left.$uid, Right.$uid).
        // TODO: This generated column is an internal name and
        // isn't selectable by the end user, this makes full outer joins
        // unusable in nested queries. Improve either error messages or change
        // query semantics around full outer joins to fix this usability gap.
        std::vector<ResolvedColumn> wrapped_column_list = copy->column_list();
        copy->add_column_list(left_uid);
        copy->add_column_list(right_uid);

        std::vector<std::unique_ptr<const ResolvedExpr>> arguments;
        arguments.emplace_back(MakeColRef(left_uid));
        arguments.emplace_back(MakeColRef(right_uid));
        ZETASQL_ASSIGN_OR_RETURN(
            std::unique_ptr<ResolvedExpr> coalesced_uid_function,
            ResolveFunctionCall("coalesce", std::move(arguments),
                                /*named_arguments=*/{}, resolver_));

        ResolvedColumn uid_column = allocator_->MakeCol(
            "$join", "$uid", coalesced_uid_function->type());
        auto coalesced_uid_column = MakeResolvedComputedColumn(
            uid_column, std::move(coalesced_uid_function));
        if (current_uid_.SetColumn(coalesced_uid_column->column())) {
          wrapped_column_list.emplace_back(current_uid_.column);
        }

        PushNodeToStack(MakeResolvedProjectScan(
            wrapped_column_list,
            MakeNodeVector(std::move(coalesced_uid_column)),
            ConsumeTopOfStack<ResolvedScan>()));

        return absl::OkStatus();
    }
  }

  // Nested AggregateScans require special handling. The differential privacy
  // spec requires that each such scan GROUPs BY the $uid column. But GROUP BY
  // columns are implemented as computed columns in ZetaSQL, so we need to
  // inspect the group by list and update 'current_uid_column_' with the new
  // ResolvedColumn.
  absl::Status VisitResolvedAggregateScan(
      const ResolvedAggregateScan* node) override {
    ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedAggregateScan(node));
    if (!current_uid_.column.IsInitialized()) {
      // Table doesn't contain any private data, so do nothing.
      return absl::OkStatus();
    }

    ResolvedAggregateScan* copy = GetUnownedTopOfStack<ResolvedAggregateScan>();

    // If the source table is a value table the uid column refs will be
    // GetProtoField or GetStructField expressions, replace them with ColumnRef
    // expressions.
    copy->set_group_by_list(current_uid_.SubstituteUidComputedColumn(
        copy->release_group_by_list()));

    // AggregateScan nodes in the per-user transform must always group by
    // $uid. Check if we already do so, and add a group by element if not.
    ResolvedColumn group_by_uid_col;
    for (const auto& col : copy->group_by_list()) {
      if (col->expr()->node_kind() != zetasql::RESOLVED_COLUMN_REF) {
        // Even if 'group by $uid+0' is equivalent to 'group by $uid', these
        // kind of operations are hard to verify so let's ignore them.
        continue;
      }
      const ResolvedColumn& grouped_by_column =
          col->expr()->GetAs<ResolvedColumnRef>()->column();
      if (grouped_by_column.column_id() == current_uid_.column.column_id()) {
        group_by_uid_col = col->column();
        break;
      }
    }

    if (group_by_uid_col.IsInitialized()) {
      // Point current_uid_column_ to the updated group by column, and verify
      // that the original query projected it.
      if (current_uid_.SetColumn(group_by_uid_col)) {
        for (const ResolvedColumn& col : copy->column_list()) {
          if (col == current_uid_.column) {
            // Explicitly projecting a column removes the alias.
            current_uid_.alias = "";
            return absl::OkStatus();
          }
        }
      }
    }
    return absl::OkStatus();
  }

  // For nested projection operations, we require the query to explicitly
  // project $uid.
  absl::Status VisitResolvedProjectScan(
      const ResolvedProjectScan* node) override {
    ZETASQL_RETURN_IF_ERROR(
        MaybeAttachParseLocation(CopyVisitResolvedProjectScan(node), *node));

    if (!current_uid_.column.IsInitialized()) {
      return absl::OkStatus();
    }
    auto* copy = GetUnownedTopOfStack<ResolvedProjectScan>();

    // If the source table is a value table the uid column refs will be
    // GetProtoField or GetStructField expressions, replace them with ColumnRef
    // expressions.
    copy->set_expr_list(
        current_uid_.SubstituteUidComputedColumn(copy->release_expr_list()));

    for (const ResolvedColumn& col : copy->column_list()) {
      if (col.column_id() == current_uid_.column.column_id()) {
        // Explicitly projecting a column removes the alias.
        current_uid_.alias = "";
        return absl::OkStatus();
      }
    }

    // TODO: Ensure that the $uid column name in the error message
    // is appropriately alias/qualified.
    return MakeSqlErrorAtNode(*copy) << absl::StrFormat(
               "Subqueries of %s queries must explicitly SELECT the userid "
               "column '%s'",
               absl::AsciiStrToLower(select_with_mode_name_.name),
               current_uid_.ToString());
  }

  absl::Status VisitResolvedSubqueryExpr(
      const ResolvedSubqueryExpr* node) override {
    // Expression subqueries aren't allowed to read from tables or TVFs that
    // have $uid columns. See (broken link)
    ExpressionSubqueryRewriterVisitor subquery_visitor;
    ZETASQL_RETURN_IF_ERROR(node->Accept(&subquery_visitor));
    ZETASQL_ASSIGN_OR_RETURN(auto copy,
                     subquery_visitor.ConsumeRootNode<ResolvedSubqueryExpr>());
    PushNodeToStack(std::move(copy));
    return absl::OkStatus();
  }

  absl::Status VisitResolvedSetOperationScan(
      const ResolvedSetOperationScan* node) override {
    std::vector<std::unique_ptr<const ResolvedSetOperationItem>>
        rewritten_input_items;
    std::vector<UidColumnState> uids;

    // Rewrite each input item.
    for (const auto& input_item : node->input_item_list()) {
      PerUserRewriterVisitor input_item_visitor(
          allocator_, type_factory_, resolver_, resolved_table_scans_,
          with_entries_, select_with_mode_name_);
      ZETASQL_RETURN_IF_ERROR(input_item->Accept(&input_item_visitor));
      UidColumnState uid = input_item_visitor.current_uid_;
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<ResolvedSetOperationItem> rewritten_input_item,
          input_item_visitor.ConsumeRootNode<ResolvedSetOperationItem>());

      if (uid.column.IsInitialized()) {
        // The $uid column should be included in the output column list, set
        // operation columns aren't trimmed at this point.
        ZETASQL_RET_CHECK(std::find(rewritten_input_item->output_column_list().begin(),
                            rewritten_input_item->output_column_list().end(),
                            uid.column) !=
                  rewritten_input_item->output_column_list().end())
            << "Column " << uid.ToString()
            << " not included in set operation output";
      }

      rewritten_input_items.push_back(std::move(rewritten_input_item));
      uids.push_back(std::move(uid));
    }

    std::unique_ptr<ResolvedSetOperationScan> copy =
        MakeResolvedSetOperationScan(node->column_list(), node->op_type(),
                                     std::move(rewritten_input_items));

    const ResolvedSetOperationItem& reference_input_item =
        *copy->input_item_list(0);
    const UidColumnState& reference_uid = uids[0];

    // Validate that either all input items have a $uid column, or that none do.
    for (int i = 1; i < copy->input_item_list_size(); ++i) {
      if (reference_uid.column.IsInitialized() !=
          uids[i].column.IsInitialized()) {
        std::string select_with_identifier_lower =
            absl::AsciiStrToLower(select_with_mode_name_.name);
        absl::string_view a_or_an =
            select_with_mode_name_.uses_a_article ? "a" : "an";
        return MakeSqlErrorAtNode(*node) << absl::StrFormat(
                   "Not all queries in %s are %s-enabled table "
                   "expressions; query 1 %s %s %s-enabled table "
                   "expression, but query %d %s",
                   SetOperationTypeToString(copy->op_type()),
                   select_with_identifier_lower,
                   reference_uid.column.IsInitialized() ? "is" : "is not",
                   a_or_an, select_with_identifier_lower, i + 1,
                   uids[i].column.IsInitialized() ? "is" : "is not");
      }
    }

    // If input items set the $uid column, ensure that they all point to the
    // same column offset.
    if (reference_uid.column.IsInitialized()) {
      std::size_t reference_uid_index =
          std::find(reference_input_item.output_column_list().begin(),
                    reference_input_item.output_column_list().end(),
                    reference_uid.column) -
          reference_input_item.output_column_list().begin();
      ZETASQL_RET_CHECK_NE(reference_uid_index,
                   reference_input_item.output_column_list_size());
      for (int i = 1; i < copy->input_item_list_size(); ++i) {
        const auto& column_list =
            copy->input_item_list(i)->output_column_list();
        std::size_t uid_index =
            std::find(column_list.begin(), column_list.end(), uids[i].column) -
            column_list.begin();
        if (reference_uid_index != uid_index) {
          return MakeSqlErrorAtNode(*node) << absl::StrFormat(
                     "Queries in %s have mismatched userid columns; query 1 "
                     "has userid column '%s' in position %d, query %d has "
                     "userid column '%s' in position %d",
                     SetOperationTypeToString(copy->op_type()),
                     reference_uid.ToString(), reference_uid_index + 1, i + 1,
                     uids[i].ToString(), uid_index + 1);
        }
      }

      current_uid_.SetColumn(
          copy->column_list(static_cast<int>(reference_uid_index)));
    }
    PushNodeToStack(std::move(copy));

    return absl::OkStatus();
  }

  /////////////////////////////////////////////////////////////////////////////
  // For these scans, the $uid column can be implicitly projected
  /////////////////////////////////////////////////////////////////////////////
#define PROJECT_UID(resolved_scan)                                        \
  absl::Status Visit##resolved_scan(const resolved_scan* node) override { \
    ZETASQL_RETURN_IF_ERROR(CopyVisit##resolved_scan(node));                      \
    if (!current_uid_.column.IsInitialized()) {                           \
      return absl::OkStatus();                                            \
    }                                                                     \
    auto* scan = GetUnownedTopOfStack<resolved_scan>();                   \
    current_uid_.ProjectIfMissing(*scan);                                 \
    return absl::OkStatus();                                              \
  }
  PROJECT_UID(ResolvedArrayScan);
  PROJECT_UID(ResolvedSingleRowScan);
  PROJECT_UID(ResolvedFilterScan);
  PROJECT_UID(ResolvedOrderByScan);
  PROJECT_UID(ResolvedLimitOffsetScan);
  PROJECT_UID(ResolvedSampleScan);
#undef PROJECT_UID

  /////////////////////////////////////////////////////////////////////////////
  // As of now unsupported per-user scans
  // TODO: Provide a user-friendly error message
  /////////////////////////////////////////////////////////////////////////////
#define UNSUPPORTED(resolved_scan)                                            \
  absl::Status Visit##resolved_scan(const resolved_scan* node) override {     \
    return MakeSqlErrorAtNode(*node)                                          \
           << "Unsupported scan type inside of SELECT WITH "                  \
           << select_with_mode_name_.name << " from clause: " #resolved_scan; \
  }
  UNSUPPORTED(ResolvedAnalyticScan);
  UNSUPPORTED(ResolvedRelationArgumentScan);
  UNSUPPORTED(ResolvedRecursiveScan);
  UNSUPPORTED(ResolvedRecursiveRefScan);
#undef UNSUPPORTED

  // Join errors are special cased because:
  // 1) they reference uid columns from two different table subqueries
  // 2) we want to suggest table names as implicit aliases, when helpful
  static std::string FormatJoinUidError(
      const absl::FormatSpec<std::string, std::string>& format_str,
      UidColumnState column1, UidColumnState column2) {
    if (IsInternalAlias(column1.column.name()) ||
        IsInternalAlias(column2.column.name())) {
      return "";
    }
    // Use full table names as uid aliases where doing so reduces ambiguity:
    // 1) the tables must have different names
    // 2) the uid columns must have the same name
    // 3) the query doesn't specify a table alias
    if (column1.column.table_name() != column2.column.table_name() &&
        column1.column.name() == column2.column.name()) {
      if (column1.alias.empty()) column1.alias = column1.column.table_name();
      if (column2.alias.empty()) column2.alias = column2.column.table_name();
    }
    return absl::StrFormat(format_str, column1.ToString(), column2.ToString());
  }

  absl::Status ValidateUidColumnSupportsGrouping(const ResolvedNode& node) {
    if (!current_uid_.column.type()->SupportsGrouping(resolver_->language())) {
      return MakeSqlErrorAtNode(node)
             << "User id columns must support grouping, instead got type "
             << Type::TypeKindToString(current_uid_.column.type()->kind(),
                                       resolver_->language().product_mode());
    }
    return absl::OkStatus();
  }

  ColumnFactory* allocator_;                                     // unowned
  TypeFactory* type_factory_;                                    // unowned
  Resolver* resolver_;                                           // unowned
  std::vector<const ResolvedTableScan*>& resolved_table_scans_;  // unowned
  std::vector<std::unique_ptr<WithEntryRewriteState>>&
      with_entries_;  // unowned

  SelectWithModeName select_with_mode_name_;
  UidColumnState current_uid_;
};

struct RewritePerUserTransformResult {
  // The rewritten per-user transform, possibly re-wrapped in another
  // ResolvedScan.
  std::unique_ptr<ResolvedScan> input_scan;

  // The original UID column extracted from the per-user transform. If original
  // UID is not a column this value may be uninitialized.
  ResolvedColumn inner_uid_column;

  // A projected intermediate column that points to inner_uid_column.
  ResolvedColumn uid_column;

  // This map is populated when the per-user aggregate list is resolved. It maps
  // the existing columns in the original DP aggregate scan `column_list` to the
  // new intermediate columns that splice together the per-user and cross-user
  // aggregate/groupby lists.
  std::map<ResolvedColumn, ResolvedColumn> injected_col_map;
};

std::unique_ptr<ResolvedScan> MakePerUserAggregateScan(
    std::unique_ptr<const ResolvedScan> input_scan,
    std::vector<std::unique_ptr<ResolvedComputedColumn>> aggregate_list,
    std::vector<std::unique_ptr<ResolvedComputedColumn>> group_by_list) {
  // Collect an updated column list, the new list will be entirely disjoint
  // from the original due to intermediate column id rewriting.
  std::vector<ResolvedColumn> new_column_list;
  new_column_list.reserve(aggregate_list.size() + group_by_list.size());
  for (const auto& column : aggregate_list) {
    new_column_list.push_back(column->column());
  }
  for (const auto& column : group_by_list) {
    new_column_list.push_back(column->column());
  }
  return MakeResolvedAggregateScan(
      new_column_list, std::move(input_scan), std::move(group_by_list),
      std::move(aggregate_list),
      /* grouping_set_list= */ {}, /* rollup_column_list= */ {}, /*grouping_sets_column_list=*/{}, /*cube_column_list=*/{});
}

absl::StatusOr<std::unique_ptr<const ResolvedExpr>>
RewriterVisitor::ChooseUidColumn(
    const ResolvedAggregateScanBase* node,
    SelectWithModeName select_with_mode_name,
    const UidColumnState& per_user_visitor_uid_column_state,
    std::optional<const ResolvedExpr*> options_uid_column) {
  if (options_uid_column.has_value()) {
    if (per_user_visitor_uid_column_state.column.IsInitialized()) {
      return MakeSqlErrorAtNode(*node)
             << "privacy_unit_column option cannot override the privacy unit "
                "column set in the table metadata: "
             << per_user_visitor_uid_column_state.ToString();
    }
    if (options_uid_column.has_value()) {
      ResolvedASTDeepCopyVisitor deep_copy_visitor;
      ZETASQL_RETURN_IF_ERROR(options_uid_column.value()->Accept(&deep_copy_visitor));
      return deep_copy_visitor.ConsumeRootNode<ResolvedExpr>();
    }
  }

  if (per_user_visitor_uid_column_state.column.IsInitialized()) {
    return MakeColRef(per_user_visitor_uid_column_state.column);
  }
  return MakeSqlErrorAtNode(*node)
         << "A SELECT WITH " << select_with_mode_name.name
         << " query must query data with a specified privacy unit column";
}

absl::StatusOr<RewritePerUserTransformResult>
RewriterVisitor::RewritePerUserTransform(
    const ResolvedAggregateScanBase* node,
    SelectWithModeName select_with_mode_name,
    std::optional<const ResolvedExpr*> options_uid_column) {
  // Construct a deep copy of the input scan, rewriting aggregates and group by
  // columns along the way, and projecting $uid to the top.
  PerUserRewriterVisitor per_user_visitor(allocator_, type_factory_, resolver_,
                                          resolved_table_scans_, with_entries_,
                                          select_with_mode_name);
  ZETASQL_RETURN_IF_ERROR(node->input_scan()->Accept(&per_user_visitor));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedScan> input_scan,
                   per_user_visitor.ConsumeRootNode<ResolvedScan>());
  std::map<ResolvedColumn, ResolvedColumn> injected_col_map;
  InnerAggregateListRewriterVisitor inner_rewriter_visitor(
      &injected_col_map, allocator_, resolver_, select_with_mode_name.name);
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<ResolvedComputedColumn>> inner_aggregate_list,
      inner_rewriter_visitor.RewriteAggregateColumns(node));
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<ResolvedComputedColumn>> inner_group_by_list,
      inner_rewriter_visitor.RewriteGroupByColumns(node));

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> inner_uid_column,
                   ChooseUidColumn(node, select_with_mode_name,
                                   per_user_visitor.uid_column_state(),
                                   std::move(options_uid_column)));

  // This is validated by PerUserRewriterVisitor.
  ZETASQL_RET_CHECK(inner_uid_column->type()->SupportsGrouping(resolver_->language()));

  // Group by the $uid column.
  ResolvedColumn uid_column =
      allocator_->MakeCol("$group_by", "$uid", inner_uid_column->type());
  inner_group_by_list.emplace_back(
      MakeResolvedComputedColumn(uid_column, std::move(inner_uid_column)));

  // We need to rewrite the ANON_VAR_POP/ANON_STDDEV_POP/ANON_PERCENTILE_CONT's
  // InnerAggregateScan to ARRAY_AGG(expr ORDER BY rand() LIMIT 5).
  // We allocated an `order_by_column` in the InnerAggregateListRewriter, the
  // `order_by_column` will be rand(). Then we can use the new_project_scan as
  // the input_scan of ResolvedAggregateScan.
  if (inner_rewriter_visitor.order_by_column().IsInitialized()) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> rand_function,
                     ResolveFunctionCall("rand", /*arguments=*/{},
                                         /*named_arguments=*/{}, resolver_));
    std::vector<std::unique_ptr<ResolvedComputedColumn>> order_by_expr_list;
    std::unique_ptr<ResolvedComputedColumn> rand_expr =
        MakeResolvedComputedColumn(inner_rewriter_visitor.order_by_column(),
                                   std::move(rand_function));
    ZETASQL_RET_CHECK(rand_expr != nullptr);
    order_by_expr_list.emplace_back(std::move(rand_expr));

    ResolvedColumnList wrapper_column_list = input_scan->column_list();
    for (const auto& computed_column : order_by_expr_list) {
      wrapper_column_list.push_back(computed_column->column());
    }
    std::unique_ptr<const ResolvedScan> new_project_scan =
        MakeResolvedProjectScan(wrapper_column_list,
                                std::move(order_by_expr_list),
                                std::move(input_scan));

    input_scan = std::move(new_project_scan);
  }

  return RewritePerUserTransformResult{
      .input_scan = MakePerUserAggregateScan(std::move(input_scan),
                                             std::move(inner_aggregate_list),
                                             std::move(inner_group_by_list)),
      .inner_uid_column = (per_user_visitor.uid_column().has_value()
                               ? *per_user_visitor.uid_column()
                               : ResolvedColumn()),
      .uid_column = uid_column,
      .injected_col_map = std::move(injected_col_map),
  };
}

absl::StatusOr<std::unique_ptr<ResolvedComputedColumn>>
RewriterVisitor::MakeGroupSelectionThresholdFunctionColumn(
    const ResolvedAnonymizedAggregateScan* scan_node) {
  std::vector<std::unique_ptr<const ResolvedExpr>> argument_list;
  // Create function call argument list logically equivalent to:
  //   ANON_SUM(1 CLAMPED BETWEEN 0 AND 1)
  argument_list.emplace_back(MakeResolvedLiteral(Value::Int64(1)));
  argument_list.emplace_back(MakeResolvedLiteral(Value::Int64(0)));
  argument_list.emplace_back(MakeResolvedLiteral(Value::Int64(1)));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> call,
                   ResolveFunctionCall("anon_sum", std::move(argument_list),
                                       /*named_arguments=*/{}, resolver_));
  ZETASQL_RET_CHECK_EQ(call->node_kind(), RESOLVED_AGGREGATE_FUNCTION_CALL)
      << call->DebugString();
  // k_threshold is old name used in ResolvedAnonymizedAggregateScan it got
  // updated to group selection threshold see: (broken link).
  ResolvedColumn uid_column =
      allocator_->MakeCol("$anon", "$k_threshold_col", call->type());
  return MakeResolvedComputedColumn(uid_column, std::move(call));
}

absl::StatusOr<std::unique_ptr<ResolvedComputedColumn>>
RewriterVisitor::MakeGroupSelectionThresholdFunctionColumn(
    const ResolvedDifferentialPrivacyAggregateScan* scan_node) {
  static const IdString contribution_bounds_per_group =
      IdString::MakeGlobal("contribution_bounds_per_group");
  std::vector<std::unique_ptr<const ResolvedExpr>> argument_list;
  // Create function call argument list logically equivalent to:
  //   SUM(1, contribution_bounds_per_group => (0, 1))
  argument_list.emplace_back(MakeResolvedLiteral(Value::Int64(1)));

  const StructType* contribution_bounds_type = nullptr;
  ZETASQL_RETURN_IF_ERROR(type_factory_->MakeStructType(
      {{"", type_factory_->get_int64()}, {"", type_factory_->get_int64()}},
      &contribution_bounds_type));
  ZETASQL_ASSIGN_OR_RETURN(auto value,
                   Value::MakeStruct(contribution_bounds_type,
                                     {Value::Int64(0), Value::Int64(1)}));
  argument_list.emplace_back(MakeResolvedLiteral(value));

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedExpr> call,
      ResolveFunctionCall(
          "$differential_privacy_sum", std::move(argument_list),
          {NamedArgumentInfo(contribution_bounds_per_group, 1, scan_node)},
          resolver_));
  ZETASQL_RET_CHECK_EQ(call->node_kind(), RESOLVED_AGGREGATE_FUNCTION_CALL)
      << call->DebugString();
  ResolvedColumn uid_column = allocator_->MakeCol(
      "$differential_privacy", "$group_selection_threshold_col", call->type());
  return MakeResolvedComputedColumn(uid_column, std::move(call));
}

std::unique_ptr<ResolvedAnonymizedAggregateScan>
RewriterVisitor::CreateAggregateScanAndUpdateScanMap(
    const ResolvedAnonymizedAggregateScan* node,
    std::unique_ptr<ResolvedScan> input_scan,
    std::vector<std::unique_ptr<ResolvedComputedColumn>> outer_group_by_list,
    std::vector<std::unique_ptr<ResolvedComputedColumn>> outer_aggregate_list,
    std::unique_ptr<ResolvedExpr> group_selection_threshold_expr,
    std::vector<std::unique_ptr<ResolvedOption>> resolved_options) {
  auto result = MakeResolvedAnonymizedAggregateScan(
      node->column_list(), std::move(input_scan),
      std::move(outer_group_by_list), std::move(outer_aggregate_list),
      std::move(group_selection_threshold_expr), std::move(resolved_options));
  for (auto resolved_table_scan : resolved_table_scans_) {
    table_scan_to_anon_aggr_scan_map_.emplace(resolved_table_scan,
                                              result.get());
  }
  return result;
}

std::unique_ptr<ResolvedDifferentialPrivacyAggregateScan>
RewriterVisitor::CreateAggregateScanAndUpdateScanMap(
    const ResolvedDifferentialPrivacyAggregateScan* node,
    std::unique_ptr<ResolvedScan> input_scan,
    std::vector<std::unique_ptr<ResolvedComputedColumn>> outer_group_by_list,
    std::vector<std::unique_ptr<ResolvedComputedColumn>> outer_aggregate_list,
    std::unique_ptr<ResolvedExpr> group_selection_threshold_expr,
    std::vector<std::unique_ptr<ResolvedOption>> resolved_options) {
  auto result = MakeResolvedDifferentialPrivacyAggregateScan(
      node->column_list(), std::move(input_scan),
      std::move(outer_group_by_list), std::move(outer_aggregate_list),
      std::move(group_selection_threshold_expr), std::move(resolved_options));
  for (auto resolved_table_scan : resolved_table_scans_) {
    table_scan_to_dp_aggr_scan_map_.emplace(resolved_table_scan, result.get());
  }
  return result;
}

// Verifies that `option` is a resolved literal containing either a valid int64_t
// value that fits into an int32_t or NULL. Returns the value of the resolved
// literal.
absl::StatusOr<Value> ValidateMaxGroupsContributed(
    const ResolvedOption& option, absl::string_view dp_option_error_prefix) {
  zetasql_base::StatusBuilder invalid_value_message =
      MakeSqlErrorAtNode(option)
      << dp_option_error_prefix << " must be an INT64 literal between 1 and "
      << std::numeric_limits<int32_t>::max();
  if (option.value()->node_kind() != RESOLVED_LITERAL ||
      !option.value()->GetAs<ResolvedLiteral>()->type()->IsInt64()) {
    return invalid_value_message;
  }
  Value max_groups_contributed =
      option.value()->GetAs<ResolvedLiteral>()->value();

  if (max_groups_contributed.is_null()) {
    return max_groups_contributed;
  } else if (!max_groups_contributed.is_valid() ||
             max_groups_contributed.int64_value() < 1 ||
             max_groups_contributed.int64_value() >
                 std::numeric_limits<int32_t>::max()) {
    // The privacy libraries only support int32_t max_groups_contributed, so
    // produce an error if the max_groups_contributed value does not fit in that
    // range.
    return invalid_value_message;
  }
  return max_groups_contributed;
}

absl::StatusOr<std::unique_ptr<ResolvedScan>>
RewriterVisitor::AddCrossPartitionSampleScan(
    std::unique_ptr<ResolvedScan> input_scan,
    std::optional<Value> max_groups_contributed,
    absl::string_view default_max_groups_contributed_option_name,
    ResolvedColumn uid_column,
    std::vector<std::unique_ptr<ResolvedOption>>&
        resolved_anonymization_options) {
  if (max_groups_contributed.has_value() && max_groups_contributed->is_null()) {
    // When max_groups_contributed is explicitly set to NULL, don't add a
    // SampleScan.
    return input_scan;
  }

  const int64_t default_max_groups_contributed =
      resolver_->analyzer_options().default_anon_kappa_value();
  ZETASQL_RET_CHECK(0 <= default_max_groups_contributed &&
            default_max_groups_contributed <
                std::numeric_limits<int32_t>::max())
      << "Default max_groups_contributed value must be an int64_t between 0 and "
      << std::numeric_limits<int32_t>::max() << ", but was "
      << default_max_groups_contributed;

  if (!max_groups_contributed.has_value() &&
      default_max_groups_contributed > 0) {
    max_groups_contributed = Value::Int64(default_max_groups_contributed);
    std::unique_ptr<ResolvedOption> max_groups_contributed_option =
        MakeResolvedOption(
            /*qualifier=*/"",
            std::string(default_max_groups_contributed_option_name),
            MakeResolvedLiteral(*max_groups_contributed));
    resolved_anonymization_options.push_back(
        std::move(max_groups_contributed_option));
  }

  // Note that if default_max_groups_contributed is 0, then
  // max_groups_contributed might still not have a value by this point.
  if (max_groups_contributed.has_value() &&
      !max_groups_contributed->is_null()) {
    std::vector<std::unique_ptr<const ResolvedExpr>> partition_by_list;
    partition_by_list.push_back(MakeColRef(uid_column));
    const std::vector<ResolvedColumn>& column_list = input_scan->column_list();
    input_scan = MakeResolvedSampleScan(
        column_list, std::move(input_scan),
        /*method=*/"RESERVOIR", MakeResolvedLiteral(*max_groups_contributed),
        ResolvedSampleScan::ROWS, /*repeatable_argument=*/nullptr,
        /*weight_column=*/nullptr, std::move(partition_by_list));
  }

  return input_scan;
}

// Provided unique_users_count_column is column with type Proto
// (AnonOutputWithReport). Since it's counting unique users we want to replace
// group selection threshold with the count from the result of this function
// which is in AnonOutputWithReport -> value -> int_value. This method returns
// an expression extracting the value from the proto via ResolvedGetProtoField.
static absl::StatusOr<std::unique_ptr<ResolvedExpr>>
MakeExtractCountFromAnonOutputWithReportProto(
    const ResolvedColumn& unique_users_count_column,
    TypeFactory& type_factory) {
  const google::protobuf::FieldDescriptor* value_field =
      AnonOutputWithReport::GetDescriptor()->FindFieldByName("value");
  ZETASQL_RET_CHECK(value_field != nullptr);

  const Type* unique_users_count_column_type = unique_users_count_column.type();
  ZETASQL_RET_CHECK_EQ(unique_users_count_column_type->kind(), TYPE_PROTO);
  ZETASQL_RET_CHECK(unique_users_count_column_type->AsProto()->descriptor() ==
            AnonOutputWithReport::GetDescriptor());

  const Type* value_field_type;
  ZETASQL_RETURN_IF_ERROR(type_factory.GetProtoFieldType(
      value_field,
      unique_users_count_column.type()->AsProto()->CatalogNamePath(),
      &value_field_type));

  const zetasql::ProtoType* value_proto_type;
  ZETASQL_RETURN_IF_ERROR(type_factory.MakeProtoType(AnonOutputValue::GetDescriptor(),
                                             &value_proto_type));

  // Extraction of the field "value" (with type AnonOutputValue)
  // from AnonOutputWithReport
  std::unique_ptr<const ResolvedExpr> get_value_expr =
      MakeResolvedGetProtoField(
          value_field_type, MakeColRef(unique_users_count_column), value_field,
          Value::Null(value_proto_type),
          /* get_has_bit=*/false, ProtoType::GetFormatAnnotation(value_field),
          /* return_default_value_when_unset=*/false);

  // "int_value" from AnonOutputValue
  // we know that this is always an integer because this code is
  // being called only for count aggregation.
  const google::protobuf::FieldDescriptor* int_value =
      AnonOutputValue::GetDescriptor()->FindFieldByName("int_value");
  ZETASQL_RET_CHECK(int_value != nullptr);

  const Type* int_value_type;
  ZETASQL_RETURN_IF_ERROR(type_factory.GetProtoFieldType(
      int_value, unique_users_count_column.type()->AsProto()->CatalogNamePath(),
      &int_value_type));

  // Extraction of the field "int_value" (with type int64_t)
  // from AnonOutputValue
  return MakeResolvedGetProtoField(
      int_value_type, std::move(get_value_expr), int_value,
      /* default_value=*/Value::Null(int_value_type),
      /* get_has_bit=*/false, ProtoType::GetFormatAnnotation(int_value),
      /* return_default_value_when_unset=*/false);
}

// Constructors for scans don't have arguments for some fields. They must be
// attached to the node after construction.
absl::Status RewriterVisitor::AttachExtraNodeFields(
    const ResolvedScan& original, ResolvedScan& copy) {
  ZETASQL_RETURN_IF_ERROR(CopyHintList(&original, &copy));
  copy.set_is_ordered(original.is_ordered());
  const auto* parse_location = original.GetParseLocationRangeOrNULL();
  if (parse_location != nullptr) {
    copy.SetParseLocationRange(*parse_location);
  }
  return absl::OkStatus();
}

// Returns an expression extracting a INT64 value for k_threshold from the JSON
// column unique_users_count_column.
//
// The value of unique_users_count_column has format
// {result: {value: $count ...} ...}.
// Since the count was pre-computed, we want to replace k_threshold_expr with
// that computed value.
static absl::StatusOr<std::unique_ptr<ResolvedExpr>>
MakeExtractCountFromAnonOutputWithReportJson(
    const ResolvedColumn& unique_users_count_column, TypeFactory& type_factory,
    Catalog& catalog, AnalyzerOptions& options) {
  // Construct ResolvedExpr for int64_t(json_query(unique_users_count_column,
  // "$.result.value"))
  const Function* json_query_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(catalog.FindFunction({std::string("json_query")},
                                       &json_query_fn, options.find_options()));
  FunctionSignature json_query_signature(
      type_factory.get_json(),
      {type_factory.get_json(), type_factory.get_string()}, FN_JSON_QUERY_JSON);
  std::vector<std::unique_ptr<const ResolvedExpr>> json_query_fn_args(2);
  json_query_fn_args[0] = MakeColRef(unique_users_count_column);
  json_query_fn_args[1] =
      MakeResolvedLiteral(types::StringType(), Value::String("$.result.value"),
                          /*has_explicit_type=*/true);

  const Function* json_to_int64_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(catalog.FindFunction(
      {std::string("int64")}, &json_to_int64_fn, options.find_options()));
  FunctionSignature json_to_int64_signature(
      type_factory.get_int64(), {type_factory.get_json()}, FN_JSON_TO_INT64);
  std::vector<std::unique_ptr<const ResolvedExpr>> json_to_int64_fn_args(1);
  json_to_int64_fn_args[0] = MakeResolvedFunctionCall(
      types::JsonType(), json_query_fn, json_query_signature,
      std::move(json_query_fn_args), ResolvedFunctionCall::DEFAULT_ERROR_MODE);

  return MakeResolvedFunctionCall(types::Int64Type(), json_to_int64_fn,
                                  json_to_int64_signature,
                                  std::move(json_to_int64_fn_args),
                                  ResolvedFunctionCall::DEFAULT_ERROR_MODE);
}

const std::vector<std::unique_ptr<const ResolvedOption>>& GetOptions(
    const ResolvedAnonymizedAggregateScan* node) {
  return node->anonymization_option_list();
}

const std::vector<std::unique_ptr<const ResolvedOption>>& GetOptions(
    const ResolvedDifferentialPrivacyAggregateScan* node) {
  return node->option_list();
}

template <class NodeType>
struct DPNodeSpecificData;

template <>
struct DPNodeSpecificData<ResolvedAnonymizedAggregateScan> {
  static bool IsMaxGroupsContributedOption(absl::string_view argument_name) {
    return zetasql_base::CaseEqual(argument_name, "kappa") ||
           zetasql_base::CaseEqual(argument_name, "max_groups_contributed");
  }
  static constexpr absl::string_view kDefaultMaxGroupsContributedOptionName =
      "max_groups_contributed";
  static constexpr absl::string_view kMaxGroupsContributedErrorPrefix =
      "Anonymization option MAX_GROUPS_CONTRIBUTED (aka KAPPA)";
  static constexpr SelectWithModeName kSelectWithModeName = {
      .name = "ANONYMIZATION", .uses_a_article = false};
};

template <>
struct DPNodeSpecificData<ResolvedDifferentialPrivacyAggregateScan> {
  static bool IsMaxGroupsContributedOption(absl::string_view argument_name) {
    return zetasql_base::CaseEqual(argument_name, "max_groups_contributed");
  }
  static constexpr absl::string_view kDefaultMaxGroupsContributedOptionName =
      "max_groups_contributed";
  static constexpr absl::string_view kMaxGroupsContributedErrorPrefix =
      "Option MAX_GROUPS_CONTRIBUTED";
  static constexpr SelectWithModeName kSelectWithModeName = {
      .name = "DIFFERENTIAL_PRIVACY", .uses_a_article = true};
};

// We don't support setting privacy unit column in WITH ANONYMIZATION OPTIONS.
absl::StatusOr<std::optional<const ResolvedExpr*>> ExtractUidColumnFromOptions(
    const ResolvedAnonymizedAggregateScan* node) {
  return std::nullopt;
}

class PrivacyUnitColumnValidator : public ResolvedASTVisitor {
 public:
  absl::Status DefaultVisit(const ResolvedNode* node) override {
    return MakeSqlErrorAtNode(*node)
           << "Unsupported privacy_unit_column definition";
  }

  absl::Status VisitResolvedColumnRef(const ResolvedColumnRef* node) override {
    return node->ChildrenAccept(this);
  }

  absl::Status VisitResolvedGetStructField(
      const ResolvedGetStructField* node) override {
    return node->ChildrenAccept(this);
  }

  absl::Status VisitResolvedGetProtoField(
      const ResolvedGetProtoField* node) override {
    return node->ChildrenAccept(this);
  }
};

// Extracts privacy unit column from WITH DIFFERENTIAL_PRIVACY OPTIONS
// privacy_unit_column option when it is present. see:
// (broken link) for details.
absl::StatusOr<std::optional<const ResolvedExpr*>> ExtractUidColumnFromOptions(
    const ResolvedDifferentialPrivacyAggregateScan* node) {
  std::optional<const ResolvedExpr*> result;
  for (const auto& option : node->option_list()) {
    if (!zetasql_base::CaseEqual(option->name(), "privacy_unit_column")) {
      continue;
    }
    if (result.has_value()) {
      return MakeSqlErrorAtNode(*option)
             << "Option privacy_unit_column must only be set once";
    }
    PrivacyUnitColumnValidator visitor;
    ZETASQL_RETURN_IF_ERROR(option->value()->Accept(&visitor));
    result = option->value();
  }
  return result;
}

template <class NodeType>
absl::Status
RewriterVisitor::VisitResolvedDifferentialPrivacyAggregateScanTemplate(
    const NodeType* node) {
  // Look for max_groups_contributed in the options.
  std::optional<Value> max_groups_contributed;
  for (const auto& option : GetOptions(node)) {
    if (DPNodeSpecificData<NodeType>::IsMaxGroupsContributedOption(
            option->name())) {
      ZETASQL_RET_CHECK(!max_groups_contributed.has_value())
          << DPNodeSpecificData<NodeType>::kMaxGroupsContributedErrorPrefix
          << " can only be set once";
      ZETASQL_ASSIGN_OR_RETURN(
          max_groups_contributed,
          ValidateMaxGroupsContributed(
              *option,
              DPNodeSpecificData<NodeType>::kMaxGroupsContributedErrorPrefix));
    }
  }

  ZETASQL_ASSIGN_OR_RETURN(auto options_uid_column, ExtractUidColumnFromOptions(node));
  // Create the per-user aggregate scan, and populate the column map.
  ZETASQL_ASSIGN_OR_RETURN(auto rewrite_per_user_result,
                   RewritePerUserTransform(
                       node, DPNodeSpecificData<NodeType>::kSelectWithModeName,
                       options_uid_column));
  auto [input_scan, inner_uid_column, uid_column, injected_col_map] =
      std::move(rewrite_per_user_result);

  OuterAggregateListRewriterVisitor outer_rewriter_visitor(
      injected_col_map, resolver_, inner_uid_column);
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<ResolvedComputedColumn>> outer_aggregate_list,
      outer_rewriter_visitor.RewriteAggregateColumns(node));

  std::unique_ptr<ResolvedExpr> group_selection_threshold_expr;

  if (std::is_same_v<NodeType, ResolvedAnonymizedAggregateScan> &&
      resolver_->language().LanguageFeatureEnabled(
          FEATURE_ANONYMIZATION_THRESHOLDING)) {
    ResolvedColumn unique_users_count_column =
        outer_rewriter_visitor.GetUniqueUserCountColumn();
    if (unique_users_count_column.IsInitialized()) {
      switch (unique_users_count_column.type()->kind()) {
        case TYPE_PROTO: {
          ZETASQL_ASSIGN_OR_RETURN(group_selection_threshold_expr,
                           MakeExtractCountFromAnonOutputWithReportProto(
                               unique_users_count_column, *type_factory_));
          break;
        }
        case TYPE_JSON: {
          // The feature FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS should be
          // enabled in order to be able to use JSON to INT64 function.
          if (resolver_->language().LanguageFeatureEnabled(
                  FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS)) {
            ZETASQL_ASSIGN_OR_RETURN(group_selection_threshold_expr,
                             MakeExtractCountFromAnonOutputWithReportJson(
                                 unique_users_count_column, *type_factory_,
                                 *catalog_, *analyzer_options_));
          }
          // If FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS isn't enabled use
          // default logic when we add additional unique users count function
          // for k_threshold_expr instead of replacement.
          break;
        }
        default:
          group_selection_threshold_expr =
              MakeColRef(unique_users_count_column);
      }
    }
  }

  if (std::is_same_v<NodeType, ResolvedDifferentialPrivacyAggregateScan> &&
      resolver_->language().LanguageFeatureEnabled(
          FEATURE_DIFFERENTIAL_PRIVACY_THRESHOLDING)) {
    ResolvedColumn unique_users_count_column =
        outer_rewriter_visitor.GetUniqueUserCountColumn();
    if (unique_users_count_column.IsInitialized()) {
      switch (unique_users_count_column.type()->kind()) {
        case TYPE_PROTO:
          break;
        default:
          group_selection_threshold_expr =
              MakeColRef(unique_users_count_column);
      }
    }
  }

  if (group_selection_threshold_expr == nullptr) {
    // If we didn't find user function matching unique users count we create
    // it ourselves.
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<ResolvedComputedColumn> group_selection_threshold_col,
        MakeGroupSelectionThresholdFunctionColumn(node));
    group_selection_threshold_expr =
        MakeColRef(group_selection_threshold_col->column());
    outer_aggregate_list.emplace_back(std::move(group_selection_threshold_col));
  }

  // GROUP BY columns in the cross-user scan are always simple column
  // references to the intermediate columns. Any computed columns are handled
  // in the per-user scan.
  std::vector<std::unique_ptr<ResolvedComputedColumn>> outer_group_by_list;
  for (const std::unique_ptr<const ResolvedComputedColumn>& group_by :
       node->group_by_list()) {
    outer_group_by_list.emplace_back(MakeResolvedComputedColumn(
        group_by->column(),
        MakeColRef(injected_col_map.at(group_by->column()))));
  }

  // Copy the options for the new anonymized aggregate scan.
  std::vector<std::unique_ptr<ResolvedOption>> resolved_anonymization_options;
  for (const std::unique_ptr<const ResolvedOption>& option : GetOptions(node)) {
    // We don't forward privacy unit column option as it will refer to invalid
    // column at this point.
    if (zetasql_base::CaseEqual(option->name(), "privacy_unit_column")) {
      continue;
    }
    ResolvedASTDeepCopyVisitor deep_copy_visitor;
    ZETASQL_RETURN_IF_ERROR(option->Accept(&deep_copy_visitor));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedOption> option_copy,
                     deep_copy_visitor.ConsumeRootNode<ResolvedOption>());
    resolved_anonymization_options.push_back(std::move(option_copy));
  }

  ZETASQL_ASSIGN_OR_RETURN(
      input_scan,
      AddCrossPartitionSampleScan(
          std::move(input_scan), max_groups_contributed,
          DPNodeSpecificData<NodeType>::kDefaultMaxGroupsContributedOptionName,
          uid_column, resolved_anonymization_options));

  auto result = CreateAggregateScanAndUpdateScanMap(
      node, std::move(input_scan), std::move(outer_group_by_list),
      std::move(outer_aggregate_list),
      std::move(group_selection_threshold_expr),
      std::move(resolved_anonymization_options));
  ZETASQL_RETURN_IF_ERROR(AttachExtraNodeFields(*node, *result));
  resolved_table_scans_.clear();
  PushNodeToStack(std::move(result));
  return absl::OkStatus();
}

// The default behavior of ResolvedASTDeepCopyVisitor copies the WITH entries
// before copying the subquery. This is backwards, we need to know if a WITH
// entry is referenced inside a SELECT WITH ANONYMIZATION node to know how it
// should be copied. Instead, WithScans are rewritten as follows:
//
// 1. Collect a list of all (at this point un-rewritten) WITH entries.
// 2. Traverse and copy the WithScan subquery, providing the WITH entries list
//    to the PerUserRewriterVisitor when a SELECT WITH ANONYMIZATION node is
//    encountered.
// 3. When a ResolvedWithRefScan is encountered during the per-user rewriting
//    stage, begin rewriting the referenced WITH entry subquery. This can
//    repeat recursively for nested WITH entries.
// 4. Nested ResolvedWithScans inside of a SELECT WITH ANONYMIZATION node are
//    rewritten immediately by PerUserRewriterVisitor and recorded into the WITH
//    entries list.
// 5. Copy non-rewritten-at-this-point WITH entries, they weren't referenced
//    during the per-user rewriting stage and don't need special handling.
absl::Status RewriterVisitor::VisitResolvedWithScan(
    const ResolvedWithScan* node) {
  // Remember the offset for the with_entry_list_size() number of nodes we add
  // to the list of all WITH entries, those are the ones we need to add back to
  // with_entry_list() after rewriting.
  std::size_t local_with_entries_offset = with_entries_.size();
  for (const std::unique_ptr<const ResolvedWithEntry>& entry :
       node->with_entry_list()) {
    with_entries_.emplace_back(new WithEntryRewriteState(
        {.original_entry = *entry, .rewritten_entry = nullptr}));
  }
  // Copy the subquery. This will visit and copy referenced WITH entries.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedScan> subquery,
                   ProcessNode(node->query()));

  // Extract (and rewrite if needed) the WITH entries belonging to this node
  // out of the WITH entries list.
  std::vector<std::unique_ptr<const ResolvedWithEntry>> copied_entries;
  for (std::size_t i = local_with_entries_offset;
       i < local_with_entries_offset + node->with_entry_list_size(); ++i) {
    WithEntryRewriteState& entry = *with_entries_[i];
    if (entry.rewritten_entry == nullptr) {
      // Copy unreferenced WITH entries.
      ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedWithEntry(&entry.original_entry));
      entry.rewritten_entry_owned = ConsumeTopOfStack<ResolvedWithEntry>();
      entry.rewritten_entry = entry.rewritten_entry_owned.get();
    }
    copied_entries.emplace_back(std::move(entry.rewritten_entry_owned));
  }
  ZETASQL_RET_CHECK_EQ(copied_entries.size(), node->with_entry_list_size());

  // Copy the with scan now that we have the subquery and WITH entry list
  // copied.
  auto copy =
      MakeResolvedWithScan(node->column_list(), std::move(copied_entries),
                           std::move(subquery), node->recursive());

  // Copy node members that aren't constructor arguments.
  ZETASQL_RETURN_IF_ERROR(AttachExtraNodeFields(*node, *copy));

  // Add the non-abstract node to the stack.
  PushNodeToStack(std::move(copy));
  return absl::OkStatus();
}

absl::Status RewriterVisitor::VisitResolvedProjectScan(
    const ResolvedProjectScan* node) {
  return MaybeAttachParseLocation(CopyVisitResolvedProjectScan(node), *node);
}

absl::StatusOr<std::unique_ptr<const ResolvedNode>> RewriteInternal(
    const ResolvedNode& tree, AnalyzerOptions options,
    ColumnFactory& column_factory, Catalog& catalog, TypeFactory& type_factory,
    RewriteForAnonymizationOutput::TableScanToAnonAggrScanMap&
        table_scan_to_anon_aggr_scan_map,
    RewriteForAnonymizationOutput::TableScanToDPAggrScanMap&
        table_scan_to_dp_aggr_scan_map) {
  options.CreateDefaultArenasIfNotSet();

  Resolver resolver(&catalog, &type_factory, &options);
  // The fresh resolver needs to be reset to initialize internal state before
  // use. We can use an empty SQL string because we aren't resolving a query,
  // we are just using the resolver to help resolve function calls from the
  // catalog.
  // Normally if errors are encountered during the function resolving process
  // the resolver also returns error locations based on the query string. We
  // don't have this issue because the calling code ensures that the resolve
  // calls do not return errors during normal use. We construct bogus
  // locations when resolving functions so that the resolver doesn't segfault
  // if an error is encountered, the bogus location information is ok because
  // these errors should only be raised during development in this file.
  resolver.Reset("");

  RewriterVisitor rewriter(&column_factory, &type_factory, &resolver,
                           table_scan_to_anon_aggr_scan_map,
                           table_scan_to_dp_aggr_scan_map, &catalog, &options);
  ZETASQL_RETURN_IF_ERROR(tree.Accept(&rewriter));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedNode> node,
                   rewriter.ConsumeRootNode<ResolvedNode>());
  return node;
}

}  // namespace

class AnonymizationRewriter : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.AllArenasAreInitialized());
    ColumnFactory column_factory(0, options.id_string_pool().get(),
                                 options.column_id_sequence_number());
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedNode> node,
        RewriteInternal(
            input, options, column_factory, catalog, type_factory,
            output_properties
                .resolved_table_scan_to_anonymized_aggregate_scan_map,
            output_properties.resolved_table_scan_to_dp_aggregate_scan_map));
    return node;
  }

  std::string Name() const override { return "AnonymizationRewriter"; }
};

absl::StatusOr<RewriteForAnonymizationOutput> RewriteForAnonymization(
    const ResolvedNode& query, Catalog* catalog, TypeFactory* type_factory,
    const AnalyzerOptions& analyzer_options, ColumnFactory& column_factory) {
  RewriteForAnonymizationOutput result;
  ZETASQL_ASSIGN_OR_RETURN(
      result.node,
      RewriteInternal(query, analyzer_options, column_factory, *catalog,
                      *type_factory, result.table_scan_to_anon_aggr_scan_map,
                      result.table_scan_to_dp_aggr_scan_map));
  return result;
}

const Rewriter* GetAnonymizationRewriter() {
  static const Rewriter* kRewriter = new AnonymizationRewriter;
  return kRewriter;
}

}  // namespace zetasql
