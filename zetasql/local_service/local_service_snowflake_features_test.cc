#include "zetasql/local_service/local_service.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/base/path.h"
#include "google/protobuf/wrappers.pb.h"
#include "google/protobuf/compiler/importer.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/text_format.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/common/testing/proto_matchers.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/common/testing/testing_proto_util.h"
#include "zetasql/proto/function.pb.h"
#include "zetasql/proto/simple_catalog.pb.h"
#include "zetasql/public/formatter_options.pb.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/parse_resume_location.pb.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/simple_table.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/public/value.pb.h"
#include "zetasql/resolved_ast/resolved_ast.pb.h"
#include "zetasql/testdata/test_proto3.pb.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/status.h"

namespace zetasql {

using ::zetasql::testing::EqualsProto;
using ::testing::IsEmpty;
using ::testing::Not;
using ::zetasql_base::testing::IsOk;
using ::zetasql_base::testing::StatusIs;
namespace local_service {

class ZetaSqlLocalServiceImplTest : public ::testing::Test {
 protected:
  void SetUp() override {
    source_tree_ = CreateProtoSourceTree();
    proto_importer_ = std::make_unique<google::protobuf::compiler::Importer>(
        source_tree_.get(), nullptr);
    ASSERT_NE(nullptr, proto_importer_->Import(
                           "zetasql/testdata/test_schema.proto"));
    pool_ = std::make_unique<google::protobuf::DescriptorPool>(proto_importer_->pool());
    // We expect 1, the builtin descriptor pool.
    EXPECT_EQ(1, service_.NumRegisteredDescriptorPools());
    EXPECT_EQ(0, service_.NumRegisteredCatalogs());
    EXPECT_EQ(0, service_.NumSavedPreparedExpression());
    EXPECT_EQ(0, service_.NumSavedPreparedQueries());
    EXPECT_EQ(0, service_.NumSavedPreparedModifies());
  }

  void TearDown() override {
    // We expect 1, the builtin descriptor pool.
    EXPECT_EQ(1, service_.NumRegisteredDescriptorPools());
    EXPECT_EQ(0, service_.NumRegisteredCatalogs());
    EXPECT_EQ(0, service_.NumSavedPreparedExpression());
    EXPECT_EQ(0, service_.NumSavedPreparedQueries());
    EXPECT_EQ(0, service_.NumSavedPreparedModifies());
  }

  absl::Status Analyze(const AnalyzeRequest& request,
                       AnalyzeResponse* response) {
    return service_.Analyze(request, response);
  }

  SimpleCatalogProto GetPreparedSimpleCatalogProto() {
    const std::string catalog_proto_text = R"pb(
        name: "test_catalog"
        table {
          name: "table_1"
          serialization_id: 1
          column {
            name: "column_1"
            type { type_kind: TYPE_INT32 }
            is_pseudo_column: false
          }
          column {
            name: "column_2"
            type { type_kind: TYPE_STRING }
            is_pseudo_column: false
          }
          column {
            name: "bool_column"
            type { type_kind: TYPE_BOOL }
            is_pseudo_column: false
          }
        })pb";

    SimpleCatalogProto catalog;
    ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(catalog_proto_text, &catalog));

    zetasql::ZetaSQLBuiltinFunctionOptionsProto options;
    options.mutable_language_options()->add_enabled_language_features(LanguageFeature::FEATURE_V_1_2_CIVIL_TIME);
    options.mutable_language_options()->add_enabled_language_features(LanguageFeature::FEATURE_V_1_3_DECIMAL_ALIAS);
    options.mutable_language_options()->add_enabled_language_features(LanguageFeature::FEATURE_NUMERIC_TYPE);

    zetasql::ZetaSQLBuiltinFunctionOptionsProto* builtin_function_options =
        catalog.mutable_builtin_function_options();
    *builtin_function_options = options;

    return catalog;
  }

  ZetaSqlLocalServiceImpl service_;
  std::unique_ptr<google::protobuf::compiler::DiskSourceTree> source_tree_;
  std::unique_ptr<google::protobuf::compiler::Importer> proto_importer_;
  std::unique_ptr<google::protobuf::DescriptorPool> pool_;
  TypeFactory factory_;
};

// TEST_F(ZetaSqlLocalServiceImplTest, AnalyzeExpressionWithTopClause) {
//   SimpleCatalogProto catalog = GetPreparedSimpleCatalogProto();

//   AnalyzeRequest request;
//   *request.mutable_simple_catalog() = catalog;
//   request.set_sql_statement("SELECT TOP 3 column_1 FROM table_1");

//   AnalyzeResponse response;
//   ZETASQL_EXPECT_OK(Analyze(request, &response));

//   AnyResolvedExprProto responseTop = response
//       .resolved_statement()
//       .resolved_query_stmt_node()
//       .query()
//       .resolved_top_scan_node()
//       .top();

//   AnyResolvedExprProto expectedResponseTop;
//   ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
//   R"pb(resolved_literal_node {
//          parent {
//            type {
//              type_kind: TYPE_INT64
//            }
//            type_annotation_map {
//            }
//          }
//          value {
//            type {
//              type_kind: TYPE_INT64
//            }
//            value {
//              int64_value: 3
//            }
//          }
//          has_explicit_type: false
//          float_literal_id: 0
//          preserve_in_literal_remover: false
//        })pb",
//       &expectedResponseTop));
//   EXPECT_THAT(responseTop, EqualsProto(expectedResponseTop));
// }

// TEST_F(ZetaSqlLocalServiceImplTest, AnalyzeExpressionWithGroupByGroupingSetsClause) {
//   SimpleCatalogProto catalog = GetPreparedSimpleCatalogProto();

//   AnalyzeRequest request;
//   *request.mutable_simple_catalog() = catalog;
//   request.set_sql_statement("select count(*), column_1, column_2 from table_1 group by grouping sets (column_1, column_2)");

//   AnalyzeResponse response;
//   ZETASQL_EXPECT_OK(Analyze(request, &response));

//   AnyResolvedAggregateScanBaseProto responseAggregateScanBaseNode = response
//       .resolved_statement()
//       .resolved_query_stmt_node()
//       .query()
//       .resolved_project_scan_node()
//       .input_scan()
//       .resolved_aggregate_scan_base_node();

//   AnyResolvedAggregateScanBaseProto expectedAggregateScanBaseNode;
//   ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
//       R"pb(resolved_aggregate_scan_node {
//         parent {
//             parent {
//             column_list {
//                 column_id: 4
//                 table_name: "$groupby"
//                 name: "column_1"
//                 type {
//                 type_kind: TYPE_INT32
//                 }
//             }
//             column_list {
//                 column_id: 5
//                 table_name: "$groupby"
//                 name: "column_2"
//                 type {
//                 type_kind: TYPE_STRING
//                 }
//             }
//             column_list {
//                 column_id: 3
//                 table_name: "$aggregate"
//                 name: "$agg1"
//                 type {
//                 type_kind: TYPE_INT64
//                 }
//             }
//             is_ordered: false
//             }
//             input_scan {
//             resolved_table_scan_node {
//                 parent {
//                 column_list {
//                     column_id: 1
//                     table_name: "table_1"
//                     name: "column_1"
//                     type {
//                     type_kind: TYPE_INT32
//                     }
//                 }
//                 column_list {
//                     column_id: 2
//                     table_name: "table_1"
//                     name: "column_2"
//                     type {
//                     type_kind: TYPE_STRING
//                     }
//                 }
//                 is_ordered: false
//                 }
//                 table {
//                 name: "table_1"
//                 serialization_id: 1
//                 full_name: "table_1"
//                 }
//                 column_index_list: 0
//                 column_index_list: 1
//                 alias: ""
//             }
//             }
//             group_by_list {
//             column {
//                 column_id: 4
//                 table_name: "$groupby"
//                 name: "column_1"
//                 type {
//                 type_kind: TYPE_INT32
//                 }
//             }
//             expr {
//                 resolved_column_ref_node {
//                 parent {
//                     type {
//                     type_kind: TYPE_INT32
//                     }
//                     type_annotation_map {
//                     }
//                 }
//                 column {
//                     column_id: 1
//                     table_name: "table_1"
//                     name: "column_1"
//                     type {
//                     type_kind: TYPE_INT32
//                     }
//                 }
//                 is_correlated: false
//                 }
//             }
//             }
//             group_by_list {
//             column {
//                 column_id: 5
//                 table_name: "$groupby"
//                 name: "column_2"
//                 type {
//                 type_kind: TYPE_STRING
//                 }
//             }
//             expr {
//                 resolved_column_ref_node {
//                 parent {
//                     type {
//                     type_kind: TYPE_STRING
//                     }
//                     type_annotation_map {
//                     }
//                 }
//                 column {
//                     column_id: 2
//                     table_name: "table_1"
//                     name: "column_2"
//                     type {
//                     type_kind: TYPE_STRING
//                     }
//                 }
//                 is_correlated: false
//                 }
//             }
//             }
//             aggregate_list {
//             column {
//                 column_id: 3
//                 table_name: "$aggregate"
//                 name: "$agg1"
//                 type {
//                 type_kind: TYPE_INT64
//                 }
//             }
//             expr {
//                 resolved_function_call_base_node {
//                 resolved_non_scalar_function_call_base_node {
//                     resolved_aggregate_function_call_node {
//                     parent {
//                         parent {
//                         parent {
//                             type {
//                             type_kind: TYPE_INT64
//                             }
//                             type_annotation_map {
//                             }
//                         }
//                         function {
//                             name: "ZetaSQL:$count_star"
//                         }
//                         signature {
//                             return_type {
//                             kind: ARG_TYPE_FIXED
//                             type {
//                                 type_kind: TYPE_INT64
//                             }
//                             options {
//                                 cardinality: REQUIRED
//                                 extra_relation_input_columns_allowed: true
//                             }
//                             num_occurrences: 1
//                             }
//                             context_id: 57
//                             options {
//                             is_deprecated: false
//                             }
//                         }
//                         error_mode: DEFAULT_ERROR_MODE
//                         }
//                         distinct: false
//                         null_handling_modifier: DEFAULT_NULL_HANDLING
//                     }
//                     function_call_info {
//                     }
//                     }
//                 }
//                 }
//             }
//             }
//         }
//         grouping_sets_column_list {
//             parent {
//             type {
//                 type_kind: TYPE_INT32
//             }
//             type_annotation_map {
//             }
//             }
//             column {
//             column_id: 4
//             table_name: "$groupby"
//             name: "column_1"
//             type {
//                 type_kind: TYPE_INT32
//             }
//             }
//             is_correlated: false
//         }
//         grouping_sets_column_list {
//             parent {
//             type {
//                 type_kind: TYPE_STRING
//             }
//             type_annotation_map {
//             }
//             }
//             column {
//             column_id: 5
//             table_name: "$groupby"
//             name: "column_2"
//             type {
//                 type_kind: TYPE_STRING
//             }
//             }
//             is_correlated: false
//         }
//         })pb",
//       &expectedAggregateScanBaseNode));
//   EXPECT_THAT(responseAggregateScanBaseNode, EqualsProto(expectedAggregateScanBaseNode));
// }

// TEST_F(ZetaSqlLocalServiceImplTest, AnalyzeExpressionWithFetchClause) {
//   SimpleCatalogProto catalog = GetPreparedSimpleCatalogProto();

//   AnalyzeRequest request;
//   *request.mutable_simple_catalog() = catalog;
//   request.set_sql_statement("select * from table_1 offset 2 rows fetch next 3 rows only");

//   AnalyzeResponse response;
//   ZETASQL_EXPECT_OK(Analyze(request, &response));

//   ResolvedOffsetFetchScanProto resolvedOffsetFetch = response
//       .resolved_statement()
//       .resolved_query_stmt_node()
//       .query()
//       .resolved_offset_fetch_scan_node();

//   ResolvedOffsetFetchScanProto expectedOffsetFetch;
//   ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
//   R"pb(parent {
//   column_list {
//     column_id: 1
//     table_name: "table_1"
//     name: "column_1"
//     type {
//       type_kind: TYPE_INT32
//     }
//   }
//   column_list {
//     column_id: 2
//     table_name: "table_1"
//     name: "column_2"
//     type {
//       type_kind: TYPE_STRING
//     }
//   }
//   is_ordered: false
// }
// input_scan {
//   resolved_project_scan_node {
//     parent {
//       column_list {
//         column_id: 1
//         table_name: "table_1"
//         name: "column_1"
//         type {
//           type_kind: TYPE_INT32
//         }
//       }
//       column_list {
//         column_id: 2
//         table_name: "table_1"
//         name: "column_2"
//         type {
//           type_kind: TYPE_STRING
//         }
//       }
//       is_ordered: false
//     }
//     input_scan {
//       resolved_table_scan_node {
//         parent {
//           column_list {
//             column_id: 1
//             table_name: "table_1"
//             name: "column_1"
//             type {
//               type_kind: TYPE_INT32
//             }
//           }
//           column_list {
//             column_id: 2
//             table_name: "table_1"
//             name: "column_2"
//             type {
//               type_kind: TYPE_STRING
//             }
//           }
//           is_ordered: false
//         }
//         table {
//           name: "table_1"
//           serialization_id: 1
//           full_name: "table_1"
//         }
//         column_index_list: 0
//         column_index_list: 1
//         alias: ""
//       }
//     }
//   }
// }
// offset {
//   resolved_literal_node {
//     parent {
//       type {
//         type_kind: TYPE_INT64
//       }
//       type_annotation_map {
//       }
//     }
//     value {
//       type {
//         type_kind: TYPE_INT64
//       }
//       value {
//         int64_value: 2
//       }
//     }
//     has_explicit_type: false
//     float_literal_id: 0
//     preserve_in_literal_remover: false
//   }
// }
// fetch {
//   resolved_literal_node {
//     parent {
//       type {
//         type_kind: TYPE_INT64
//       }
//       type_annotation_map {
//       }
//     }
//     value {
//       type {
//         type_kind: TYPE_INT64
//       }
//       value {
//         int64_value: 3
//       }
//     }
//     has_explicit_type: false
//     float_literal_id: 0
//     preserve_in_literal_remover: false
//   }
// })pb",
//       &expectedOffsetFetch));
//   EXPECT_THAT(resolvedOffsetFetch, EqualsProto(expectedOffsetFetch));
// }

// TEST_F(ZetaSqlLocalServiceImplTest, AnalyzeExpressionWithLateralClause) {
//   SimpleCatalogProto catalog = GetPreparedSimpleCatalogProto();

//   AnalyzeRequest request;
//   *request.mutable_simple_catalog() = catalog;
//   request.set_sql_statement("select column_1 from table_1, lateral (select column_2 from table_1)");

//   AnalyzeResponse response;
//   ZETASQL_EXPECT_OK(Analyze(request, &response));

//   ResolvedJoinScanProto resolvedJoinScanProto = response
//       .resolved_statement()
//       .resolved_query_stmt_node()
//       .query()
//       .resolved_project_scan_node()
//       .input_scan()
//       .resolved_join_scan_node();

//   ResolvedJoinScanProto expectedJoinScanProto;
//   ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
//   R"pb(parent {
//         column_list {
//         column_id: 1
//         table_name: "table_1"
//         name: "column_1"
//         type {
//             type_kind: TYPE_INT32
//         }
//         }
//         column_list {
//         column_id: 2
//         table_name: "table_1"
//         name: "column_2"
//         type {
//             type_kind: TYPE_STRING
//         }
//         }
//         column_list {
//         column_id: 4
//         table_name: "table_1"
//         name: "column_2"
//         type {
//             type_kind: TYPE_STRING
//         }
//         }
//         is_ordered: false
//     }
//     join_type: INNER
//     left_scan {
//         resolved_table_scan_node {
//         parent {
//             column_list {
//             column_id: 1
//             table_name: "table_1"
//             name: "column_1"
//             type {
//                 type_kind: TYPE_INT32
//             }
//             }
//             column_list {
//             column_id: 2
//             table_name: "table_1"
//             name: "column_2"
//             type {
//                 type_kind: TYPE_STRING
//             }
//             }
//             is_ordered: false
//         }
//         table {
//             name: "table_1"
//             serialization_id: 1
//             full_name: "table_1"
//         }
//         column_index_list: 0
//         column_index_list: 1
//         alias: ""
//         }
//     }
//     right_scan {
//         resolved_project_scan_node {
//         parent {
//             column_list {
//             column_id: 4
//             table_name: "table_1"
//             name: "column_2"
//             type {
//                 type_kind: TYPE_STRING
//             }
//             }
//             is_ordered: false
//         }
//         input_scan {
//             resolved_table_scan_node {
//             parent {
//                 column_list {
//                 column_id: 3
//                 table_name: "table_1"
//                 name: "column_1"
//                 type {
//                     type_kind: TYPE_INT32
//                 }
//                 }
//                 column_list {
//                 column_id: 4
//                 table_name: "table_1"
//                 name: "column_2"
//                 type {
//                     type_kind: TYPE_STRING
//                 }
//                 }
//                 is_ordered: false
//             }
//             table {
//                 name: "table_1"
//                 serialization_id: 1
//                 full_name: "table_1"
//             }
//             column_index_list: 0
//             column_index_list: 1
//             alias: ""
//             }
//         }
//         }
//     }
//     lateral: true)pb",
//       &expectedJoinScanProto));
//   EXPECT_THAT(resolvedJoinScanProto, EqualsProto(expectedJoinScanProto));
// }

// TEST_F(ZetaSqlLocalServiceImplTest, AnalyzeExpressionWithGroupByCubeClause) {
//   SimpleCatalogProto catalog = GetPreparedSimpleCatalogProto();

//   AnalyzeRequest request;
//   *request.mutable_simple_catalog() = catalog;
//   request.set_sql_statement("select count(*) from table_1 group by cube (column_1, column_2)");

//   AnalyzeResponse response;
//   ZETASQL_EXPECT_OK(Analyze(request, &response));

//   AnyResolvedAggregateScanBaseProto responseAggregateScanBaseNode = response
//       .resolved_statement()
//       .resolved_query_stmt_node()
//       .query()
//       .resolved_project_scan_node()
//       .input_scan()
//       .resolved_aggregate_scan_base_node();

//   AnyResolvedAggregateScanBaseProto expectedAggregateScanBaseNode;
//   ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
//       R"pb(resolved_aggregate_scan_node {
//     parent {
//         parent {
//         column_list {
//             column_id: 4
//             table_name: "$groupby"
//             name: "column_1"
//             type {
//             type_kind: TYPE_INT32
//             }
//         }
//         column_list {
//             column_id: 5
//             table_name: "$groupby"
//             name: "column_2"
//             type {
//             type_kind: TYPE_STRING
//             }
//         }
//         column_list {
//             column_id: 3
//             table_name: "$aggregate"
//             name: "$agg1"
//             type {
//             type_kind: TYPE_INT64
//             }
//         }
//         is_ordered: false
//         }
//         input_scan {
//         resolved_table_scan_node {
//             parent {
//             column_list {
//                 column_id: 1
//                 table_name: "table_1"
//                 name: "column_1"
//                 type {
//                 type_kind: TYPE_INT32
//                 }
//             }
//             column_list {
//                 column_id: 2
//                 table_name: "table_1"
//                 name: "column_2"
//                 type {
//                 type_kind: TYPE_STRING
//                 }
//             }
//             is_ordered: false
//             }
//             table {
//             name: "table_1"
//             serialization_id: 1
//             full_name: "table_1"
//             }
//             column_index_list: 0
//             column_index_list: 1
//             alias: ""
//         }
//         }
//         group_by_list {
//         column {
//             column_id: 4
//             table_name: "$groupby"
//             name: "column_1"
//             type {
//             type_kind: TYPE_INT32
//             }
//         }
//         expr {
//             resolved_column_ref_node {
//             parent {
//                 type {
//                 type_kind: TYPE_INT32
//                 }
//                 type_annotation_map {
//                 }
//             }
//             column {
//                 column_id: 1
//                 table_name: "table_1"
//                 name: "column_1"
//                 type {
//                 type_kind: TYPE_INT32
//                 }
//             }
//             is_correlated: false
//             }
//         }
//         }
//         group_by_list {
//         column {
//             column_id: 5
//             table_name: "$groupby"
//             name: "column_2"
//             type {
//             type_kind: TYPE_STRING
//             }
//         }
//         expr {
//             resolved_column_ref_node {
//             parent {
//                 type {
//                 type_kind: TYPE_STRING
//                 }
//                 type_annotation_map {
//                 }
//             }
//             column {
//                 column_id: 2
//                 table_name: "table_1"
//                 name: "column_2"
//                 type {
//                 type_kind: TYPE_STRING
//                 }
//             }
//             is_correlated: false
//             }
//         }
//         }
//         aggregate_list {
//         column {
//             column_id: 3
//             table_name: "$aggregate"
//             name: "$agg1"
//             type {
//             type_kind: TYPE_INT64
//             }
//         }
//         expr {
//             resolved_function_call_base_node {
//             resolved_non_scalar_function_call_base_node {
//                 resolved_aggregate_function_call_node {
//                 parent {
//                     parent {
//                     parent {
//                         type {
//                         type_kind: TYPE_INT64
//                         }
//                         type_annotation_map {
//                         }
//                     }
//                     function {
//                         name: "ZetaSQL:$count_star"
//                     }
//                     signature {
//                         return_type {
//                         kind: ARG_TYPE_FIXED
//                         type {
//                             type_kind: TYPE_INT64
//                         }
//                         options {
//                             cardinality: REQUIRED
//                             extra_relation_input_columns_allowed: true
//                         }
//                         num_occurrences: 1
//                         }
//                         context_id: 57
//                         options {
//                         is_deprecated: false
//                         }
//                     }
//                     error_mode: DEFAULT_ERROR_MODE
//                     }
//                     distinct: false
//                     null_handling_modifier: DEFAULT_NULL_HANDLING
//                 }
//                 function_call_info {
//                 }
//                 }
//             }
//             }
//         }
//         }
//     }
//     cube_column_list {
//         parent {
//         type {
//             type_kind: TYPE_INT32
//         }
//         type_annotation_map {
//         }
//         }
//         column {
//         column_id: 4
//         table_name: "$groupby"
//         name: "column_1"
//         type {
//             type_kind: TYPE_INT32
//         }
//         }
//         is_correlated: false
//     }
//     cube_column_list {
//         parent {
//         type {
//             type_kind: TYPE_STRING
//         }
//         type_annotation_map {
//         }
//         }
//         column {
//         column_id: 5
//         table_name: "$groupby"
//         name: "column_2"
//         type {
//             type_kind: TYPE_STRING
//         }
//         }
//         is_correlated: false
//     }
//     })pb",
//       &expectedAggregateScanBaseNode));
//   EXPECT_THAT(responseAggregateScanBaseNode, EqualsProto(expectedAggregateScanBaseNode));
// }

// TEST_F(ZetaSqlLocalServiceImplTest, AnalyzeExpressionWithSelectListAlias) {
//   SimpleCatalogProto catalog = GetPreparedSimpleCatalogProto();

//   AnalyzeRequest request;
//   *request.mutable_simple_catalog() = catalog;
//   request.set_sql_statement("SELECT 0.23 AS a, 100 * a as b, a * b");

//   AnalyzeResponse response;
//   ZETASQL_EXPECT_OK(Analyze(request, &response));

//   AnalyzeResponse expectedResponse;
//   ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
//       R"pb(resolved_statement {
//   resolved_query_stmt_node {
//     output_column_list {
//       name: "a"
//       column {
//         column_id: 1
//         table_name: "$query"
//         name: "a"
//         type {
//           type_kind: TYPE_DOUBLE
//         }
//       }
//     }
//     output_column_list {
//       name: "b"
//       column {
//         column_id: 2
//         table_name: "$query"
//         name: "b"
//         type {
//           type_kind: TYPE_DOUBLE
//         }
//       }
//     }
//     output_column_list {
//       name: "$col3"
//       column {
//         column_id: 3
//         table_name: "$query"
//         name: "$col3"
//         type {
//           type_kind: TYPE_DOUBLE
//         }
//       }
//     }
//     is_value_table: false
//     query {
//       resolved_project_scan_node {
//         parent {
//           column_list {
//             column_id: 1
//             table_name: "$query"
//             name: "a"
//             type {
//               type_kind: TYPE_DOUBLE
//             }
//           }
//           column_list {
//             column_id: 2
//             table_name: "$query"
//             name: "b"
//             type {
//               type_kind: TYPE_DOUBLE
//             }
//           }
//           column_list {
//             column_id: 3
//             table_name: "$query"
//             name: "$col3"
//             type {
//               type_kind: TYPE_DOUBLE
//             }
//           }
//           is_ordered: false
//         }
//         expr_list {
//           column {
//             column_id: 1
//             table_name: "$query"
//             name: "a"
//             type {
//               type_kind: TYPE_DOUBLE
//             }
//           }
//           expr {
//             resolved_literal_node {
//               parent {
//                 type {
//                   type_kind: TYPE_DOUBLE
//                 }
//                 type_annotation_map {
//                 }
//               }
//               value {
//                 type {
//                   type_kind: TYPE_DOUBLE
//                 }
//                 value {
//                   double_value: 0.23
//                 }
//               }
//               has_explicit_type: false
//               float_literal_id: 0
//               preserve_in_literal_remover: false
//             }
//           }
//         }
//         expr_list {
//           column {
//             column_id: 2
//             table_name: "$query"
//             name: "b"
//             type {
//               type_kind: TYPE_DOUBLE
//             }
//           }
//           expr {
//             resolved_function_call_base_node {
//               resolved_function_call_node {
//                 parent {
//                   parent {
//                     type {
//                       type_kind: TYPE_DOUBLE
//                     }
//                     type_annotation_map {
//                     }
//                   }
//                   function {
//                     name: "ZetaSQL:$multiply"
//                   }
//                   signature {
//                     argument {
//                       kind: ARG_TYPE_FIXED
//                       type {
//                         type_kind: TYPE_DOUBLE
//                       }
//                       options {
//                         cardinality: REQUIRED
//                         extra_relation_input_columns_allowed: true
//                       }
//                       num_occurrences: 1
//                     }
//                     argument {
//                       kind: ARG_TYPE_FIXED
//                       type {
//                         type_kind: TYPE_DOUBLE
//                       }
//                       options {
//                         cardinality: REQUIRED
//                         extra_relation_input_columns_allowed: true
//                       }
//                       num_occurrences: 1
//                     }
//                     return_type {
//                       kind: ARG_TYPE_FIXED
//                       type {
//                         type_kind: TYPE_DOUBLE
//                       }
//                       options {
//                         cardinality: REQUIRED
//                         extra_relation_input_columns_allowed: true
//                       }
//                       num_occurrences: 1
//                     }
//                     context_id: 41
//                     options {
//                       is_deprecated: false
//                     }
//                   }
//                   argument_list {
//                     resolved_literal_node {
//                       parent {
//                         type {
//                           type_kind: TYPE_DOUBLE
//                         }
//                         type_annotation_map {
//                         }
//                       }
//                       value {
//                         type {
//                           type_kind: TYPE_DOUBLE
//                         }
//                         value {
//                           double_value: 100
//                         }
//                       }
//                       has_explicit_type: false
//                       float_literal_id: 0
//                       preserve_in_literal_remover: false
//                     }
//                   }
//                   argument_list {
//                     resolved_column_ref_node {
//                       parent {
//                         type {
//                           type_kind: TYPE_DOUBLE
//                         }
//                         type_annotation_map {
//                         }
//                       }
//                       column {
//                         column_id: 1
//                         table_name: "$query"
//                         name: "a"
//                         type {
//                           type_kind: TYPE_DOUBLE
//                         }
//                       }
//                       is_correlated: false
//                     }
//                   }
//                   error_mode: DEFAULT_ERROR_MODE
//                 }
//                 function_call_info {
//                 }
//               }
//             }
//           }
//         }
//         expr_list {
//           column {
//             column_id: 3
//             table_name: "$query"
//             name: "$col3"
//             type {
//               type_kind: TYPE_DOUBLE
//             }
//           }
//           expr {
//             resolved_function_call_base_node {
//               resolved_function_call_node {
//                 parent {
//                   parent {
//                     type {
//                       type_kind: TYPE_DOUBLE
//                     }
//                     type_annotation_map {
//                     }
//                   }
//                   function {
//                     name: "ZetaSQL:$multiply"
//                   }
//                   signature {
//                     argument {
//                       kind: ARG_TYPE_FIXED
//                       type {
//                         type_kind: TYPE_DOUBLE
//                       }
//                       options {
//                         cardinality: REQUIRED
//                         extra_relation_input_columns_allowed: true
//                       }
//                       num_occurrences: 1
//                     }
//                     argument {
//                       kind: ARG_TYPE_FIXED
//                       type {
//                         type_kind: TYPE_DOUBLE
//                       }
//                       options {
//                         cardinality: REQUIRED
//                         extra_relation_input_columns_allowed: true
//                       }
//                       num_occurrences: 1
//                     }
//                     return_type {
//                       kind: ARG_TYPE_FIXED
//                       type {
//                         type_kind: TYPE_DOUBLE
//                       }
//                       options {
//                         cardinality: REQUIRED
//                         extra_relation_input_columns_allowed: true
//                       }
//                       num_occurrences: 1
//                     }
//                     context_id: 41
//                     options {
//                       is_deprecated: false
//                     }
//                   }
//                   argument_list {
//                     resolved_column_ref_node {
//                       parent {
//                         type {
//                           type_kind: TYPE_DOUBLE
//                         }
//                         type_annotation_map {
//                         }
//                       }
//                       column {
//                         column_id: 1
//                         table_name: "$query"
//                         name: "a"
//                         type {
//                           type_kind: TYPE_DOUBLE
//                         }
//                       }
//                       is_correlated: false
//                     }
//                   }
//                   argument_list {
//                     resolved_column_ref_node {
//                       parent {
//                         type {
//                           type_kind: TYPE_DOUBLE
//                         }
//                         type_annotation_map {
//                         }
//                       }
//                       column {
//                         column_id: 2
//                         table_name: "$query"
//                         name: "b"
//                         type {
//                           type_kind: TYPE_DOUBLE
//                         }
//                       }
//                       is_correlated: false
//                     }
//                   }
//                   error_mode: DEFAULT_ERROR_MODE
//                 }
//                 function_call_info {
//                 }
//               }
//             }
//           }
//         }
//         input_scan {
//           resolved_single_row_scan_node {
//             parent {
//               is_ordered: false
//             }
//           }
//         }
//       }
//     }
//   }
// })pb",
//       &expectedResponse));
//   EXPECT_THAT(response, EqualsProto(expectedResponse));
// }

TEST_F(ZetaSqlLocalServiceImplTest, AnalyzeExpressionWithSnowflakeTypes) {
  SimpleCatalogProto catalog = GetPreparedSimpleCatalogProto();

  // NUMBER
  AnalyzeRequest analyzeNumberRequest;
  *analyzeNumberRequest.mutable_simple_catalog() = catalog;
  *analyzeNumberRequest.mutable_options()->mutable_language_options() = catalog.builtin_function_options().language_options();
  analyzeNumberRequest.set_sql_statement("SELECT CAST(1.0 AS NUMBER)");
  AnalyzeResponse analyzeNumberResponse;
  ZETASQL_EXPECT_OK(Analyze(analyzeNumberRequest, &analyzeNumberResponse));

  // INT, INTEGER, BIGINT, SMALLINT, TINYINT, BYTEINT
  AnalyzeRequest analyzeIntegerRequest;
  *analyzeIntegerRequest.mutable_simple_catalog() = catalog;
  analyzeIntegerRequest.set_sql_statement("SELECT CAST(1 AS INT), CAST(1 AS INTEGER), CAST(1 AS BIGINT), CAST(1 AS SMALLINT), CAST(1 AS TINYINT), CAST(1 AS BYTEINT)");
  AnalyzeResponse analyzeIntegerResponse;
  ZETASQL_EXPECT_OK(Analyze(analyzeIntegerRequest, &analyzeIntegerResponse));

  // FLOAT, FLOAT4, FLOAT8
  AnalyzeRequest analyzeFloatRequest;
  *analyzeFloatRequest.mutable_simple_catalog() = catalog;
  analyzeFloatRequest.set_sql_statement("SELECT CAST(1.0 AS FLOAT), CAST(1.0 AS FLOAT4), CAST(1.0 AS FLOAT8)");
  AnalyzeResponse analyzeFloatResponse;
  ZETASQL_EXPECT_OK(Analyze(analyzeFloatRequest, &analyzeFloatResponse));

  // DOUBLE PRECISION, REAL
  AnalyzeRequest analyzeRealRequest;
  *analyzeRealRequest.mutable_simple_catalog() = catalog;
  analyzeRealRequest.set_sql_statement("SELECT CAST(1.0 AS DOUBLE PRECISION), CAST(1.0 AS REAL)");
  AnalyzeResponse analyzeRealResponse;
  ZETASQL_EXPECT_OK(Analyze(analyzeRealRequest, &analyzeRealResponse));
}

TEST_F(ZetaSqlLocalServiceImplTest, AnalyzeExpressionWithSnowflakeVariantType) {
  SimpleCatalogProto catalog = GetPreparedSimpleCatalogProto();

  AnalyzeRequest analyzeNumberRequest;
  *analyzeNumberRequest.mutable_simple_catalog() = catalog;
  *analyzeNumberRequest.mutable_options()->mutable_language_options() = catalog.builtin_function_options().language_options();
  analyzeNumberRequest.set_sql_statement("SELECT CAST(CAST(1 AS VARIANT) AS INT), CAST(CAST(1.0 AS VARIANT) AS DOUBLE), CAST(CAST('str' AS VARIANT) AS STRING), CAST(CAST(true AS VARIANT) AS BOOLEAN)");
  AnalyzeResponse analyzeNumberResponse;
  ZETASQL_EXPECT_OK(Analyze(analyzeNumberRequest, &analyzeNumberResponse));
}


TEST_F(ZetaSqlLocalServiceImplTest, AnalyzeExpressionWithSnowflakeFunctions) {
  SimpleCatalogProto catalog = GetPreparedSimpleCatalogProto();

  // Snowflake Aggregate functions
  AnalyzeRequest analyzeAggregateFunctionsRequest;
  *analyzeAggregateFunctionsRequest.mutable_simple_catalog() = catalog;
  const char *aggregate_functions_request_text =
    "select "
    "approx_percentile(column_1, 0.5),"
    "approx_percentile_accumulate(column_1),"
    "approx_percentile_combine(column_1),"
    "approx_percentile_estimate(column_1, 0.01),"
    "approx_top_k(column_1), approx_top_k_accumulate(column_1, 10),"
    "approx_top_k_combine(column_1), approx_top_k_combine(column_1, 0.1),"
    "approx_top_k_estimate(column_1), approx_top_k_estimate(column_1, 0.5),"
    "approximate_jaccard_index(column_1),"
    "approximate_similarity(column_1),"
    "bitand_agg(column_1),"
    "bitor_agg(column_1),"
    "bitxor_agg(column_1),"
    "booland_agg(column_1), booland_agg(bool_column),"
    "boolor_agg(column_1), boolor_agg(bool_column),"
    "boolxor_agg(column_1), boolxor_agg(bool_column),"
    "grouping_id(column_1), grouping_id(column_1, column_2)," // TODO: grouping_id(column_1, column_2, bool_column)
    "hash_agg(null), hash_agg(null, null), hash_agg(null, null, null), hash_agg(column_1), hash_agg(column_1, column_2)," // TODO: hash_agg(*), hash_agg(column_1, column_2, bool_column)
    "hll(column_1), hll(column_1, column_2),"
    "hll_accumulate(column_1)," // TODO:  hll_accumulate(*)
    "regr_avgx(column_1, column_2), regr_avgy(column_1, column_2),"
    "regr_count(column_1, column_2) from table_1";
  analyzeAggregateFunctionsRequest.set_sql_statement(aggregate_functions_request_text);
  AnalyzeResponse analyzeAggregateFunctionsResponse;
  ZETASQL_EXPECT_OK(Analyze(analyzeAggregateFunctionsRequest, &analyzeAggregateFunctionsResponse));

  // Snowflake Bitwise functions
  AnalyzeRequest analyzeBitwiseFunctionsRequest;
  *analyzeBitwiseFunctionsRequest.mutable_simple_catalog() = catalog;
  const char *bitwise_functions_request_text =
    "select "
    "bitand(11, 22), bitnot(11), bitor(11, 22),"
    "bitshiftleft(11, 1), bitshiftright(11, 1),"
    "bitxor(11, 22)";
  analyzeBitwiseFunctionsRequest.set_sql_statement(bitwise_functions_request_text);
  AnalyzeResponse analyzeBitwiseFunctionsResponse;
  ZETASQL_EXPECT_OK(Analyze(analyzeBitwiseFunctionsRequest, &analyzeBitwiseFunctionsResponse));

  // Snowflake Conditional expression functions
  AnalyzeRequest analyzeConditionalExpressionFunctionsRequest;
  *analyzeConditionalExpressionFunctionsRequest.mutable_simple_catalog() = catalog;
  const char *conditional_expression_functions_request_text =
    "select "
    "booland(1, 0), boolnot(1),"
    "boolor(1, 0), boolxor(1, 0),"
    "zeroifnull(null)";
  analyzeConditionalExpressionFunctionsRequest.set_sql_statement(conditional_expression_functions_request_text);
  AnalyzeResponse analyzeConditionalExpressionFunctionsResponse;
  ZETASQL_EXPECT_OK(Analyze(analyzeConditionalExpressionFunctionsRequest, &analyzeConditionalExpressionFunctionsResponse));

  // Snowflake Conversion functions
  AnalyzeRequest analyzeConversionFunctionsRequest;
  *analyzeConversionFunctionsRequest.mutable_simple_catalog() = catalog;
  *analyzeConversionFunctionsRequest.mutable_options()->mutable_language_options() = catalog.builtin_function_options().language_options();
  const char *conversion_functions_request_text =
    "select "
    "to_boolean('yes'), try_to_boolean('yes'),"
    "to_double('1.1'), try_to_double('1.1'),"
    "try_to_date('2018-09-15'), try_to_time('12:30:00'),"
    "to_variant('2018-09-15'), to_variant(true), to_variant(1), to_variant(1.1), to_variant(current_date), to_variant(current_timestamp), to_variant(cast(1.1 as float))";
  analyzeConversionFunctionsRequest.set_sql_statement(conversion_functions_request_text);
  AnalyzeResponse analyzeConversionFunctionsResponse;
  ZETASQL_EXPECT_OK(Analyze(analyzeConversionFunctionsRequest, &analyzeConversionFunctionsResponse));

  // Snowflake Data generation functions
  AnalyzeRequest analyzeDataGenerationFunctionsRequest;
  *analyzeDataGenerationFunctionsRequest.mutable_simple_catalog() = catalog;
  const char *data_generation_functions_request_text =
    "select "
    "random(), randstr(5, random()),"
    "seq1(), seq2(),"
    "seq4(), seq8()";
  analyzeDataGenerationFunctionsRequest.set_sql_statement(data_generation_functions_request_text);
  AnalyzeResponse analyzeDataGenerationFunctionsResponse;
  ZETASQL_EXPECT_OK(Analyze(analyzeDataGenerationFunctionsRequest, &analyzeDataGenerationFunctionsResponse));

  // Snowflake String and binary functions
  AnalyzeRequest analyzeStringAndBinaryFunctionsRequest;
  *analyzeStringAndBinaryFunctionsRequest.mutable_simple_catalog() = catalog;
  const char *string_and_binary_functions_request_text =
    "select "
    "base64_decode_string('U25vd2ZsYWtl'), try_base64_decode_string('U25vd2ZsYWtl'),"
    "contains('ice tea', 'te'), endswith('ice tea', 'a'),"
    "insert('abcdef', 3, 2, 'zzz')";
  analyzeStringAndBinaryFunctionsRequest.set_sql_statement(string_and_binary_functions_request_text);
  AnalyzeResponse analyzeStringAndBinaryFunctionsResponse;
  ZETASQL_EXPECT_OK(Analyze(analyzeStringAndBinaryFunctionsRequest, &analyzeStringAndBinaryFunctionsResponse));

  // Snowflake String functions
  AnalyzeRequest analyzeStringFunctionsRequest;
  *analyzeStringFunctionsRequest.mutable_simple_catalog() = catalog;
  const char *string_functions_request_text =
    "select "
    "regexp_count('It was the best of times', '\\bwas\\b', 1),"
    "regexp_like(column_2, 'san.*', 'i'),"
    "regexp_substr_all('a1_a2a3_a4A5a6', 'a[[:digit:]]')"
    "from table_1";
  analyzeStringFunctionsRequest.set_sql_statement(string_functions_request_text);
  AnalyzeResponse analyzeStringFunctionsResponse;
  ZETASQL_EXPECT_OK(Analyze(analyzeStringFunctionsRequest, &analyzeStringFunctionsResponse));

  // Snowflake Date and time functions
  AnalyzeRequest analyzeDateAndTimeFunctionsRequest;
  *analyzeDateAndTimeFunctionsRequest.mutable_simple_catalog() = catalog;
  const char *date_and_time_functions_request_text =
    "select "
    "add_months(parse_date('%m/%d/%Y', '1/1/2023'), 1), dayname(parse_date('%m/%d/%Y', '1/1/2023')),"
    "monthname(PARSE_DATE('%m/%d/%Y', '1/1/2023')), next_day(parse_date('%m/%d/%Y', '1/1/2023'), 'Friday')";
  analyzeDateAndTimeFunctionsRequest.set_sql_statement(date_and_time_functions_request_text);
  AnalyzeResponse analyzeDateAndTimeFunctionsResponse;
  ZETASQL_EXPECT_OK(Analyze(analyzeDateAndTimeFunctionsRequest, &analyzeDateAndTimeFunctionsResponse));
}

}  // namespace local_service
}  // namespace zetasql
