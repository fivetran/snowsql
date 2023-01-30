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
        })pb";

    SimpleCatalogProto catalog;
    ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(catalog_proto_text, &catalog));

    zetasql::ZetaSQLBuiltinFunctionOptionsProto options;
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

TEST_F(ZetaSqlLocalServiceImplTest, AnalyzeExpressionWithTopClause) {
  SimpleCatalogProto catalog = GetPreparedSimpleCatalogProto();

  AnalyzeRequest request;
  *request.mutable_simple_catalog() = catalog;
  request.set_sql_statement("SELECT TOP 3 column_1 FROM table_1");

  AnalyzeResponse response;
  ZETASQL_EXPECT_OK(Analyze(request, &response));

  AnyResolvedExprProto responseTop = response
      .resolved_statement()
      .resolved_query_stmt_node()
      .query()
      .resolved_top_scan_node()
      .top();

  AnyResolvedExprProto expectedResponseTop;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
  R"pb(resolved_literal_node {
         parent {
           type {
             type_kind: TYPE_INT64
           }
           type_annotation_map {
           }
         }
         value {
           type {
             type_kind: TYPE_INT64
           }
           value {
             int64_value: 3
           }
         }
         has_explicit_type: false
         float_literal_id: 0
         preserve_in_literal_remover: false
       })pb",
      &expectedResponseTop));
  EXPECT_THAT(responseTop, EqualsProto(expectedResponseTop));
}

TEST_F(ZetaSqlLocalServiceImplTest, AnalyzeExpressionWithGroupByGroupingSetsClause) {
  SimpleCatalogProto catalog = GetPreparedSimpleCatalogProto();

  AnalyzeRequest request;
  *request.mutable_simple_catalog() = catalog;
  request.set_sql_statement("select count(*), column_1, column_2 from table_1 group by grouping sets (column_1, column_2)");

  AnalyzeResponse response;
  ZETASQL_EXPECT_OK(Analyze(request, &response));

  AnyResolvedAggregateScanBaseProto responseAggregateScanBaseNode = response
      .resolved_statement()
      .resolved_query_stmt_node()
      .query()
      .resolved_project_scan_node()
      .input_scan()
      .resolved_aggregate_scan_base_node();

  AnyResolvedAggregateScanBaseProto expectedAggregateScanBaseNode;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(
      R"pb(resolved_aggregate_scan_node {
        parent {
            parent {
            column_list {
                column_id: 4
                table_name: "$groupby"
                name: "column_1"
                type {
                type_kind: TYPE_INT32
                }
            }
            column_list {
                column_id: 5
                table_name: "$groupby"
                name: "column_2"
                type {
                type_kind: TYPE_STRING
                }
            }
            column_list {
                column_id: 3
                table_name: "$aggregate"
                name: "$agg1"
                type {
                type_kind: TYPE_INT64
                }
            }
            is_ordered: false
            }
            input_scan {
            resolved_table_scan_node {
                parent {
                column_list {
                    column_id: 1
                    table_name: "table_1"
                    name: "column_1"
                    type {
                    type_kind: TYPE_INT32
                    }
                }
                column_list {
                    column_id: 2
                    table_name: "table_1"
                    name: "column_2"
                    type {
                    type_kind: TYPE_STRING
                    }
                }
                is_ordered: false
                }
                table {
                name: "table_1"
                serialization_id: 1
                full_name: "table_1"
                }
                column_index_list: 0
                column_index_list: 1
                alias: ""
            }
            }
            group_by_list {
            column {
                column_id: 4
                table_name: "$groupby"
                name: "column_1"
                type {
                type_kind: TYPE_INT32
                }
            }
            expr {
                resolved_column_ref_node {
                parent {
                    type {
                    type_kind: TYPE_INT32
                    }
                    type_annotation_map {
                    }
                }
                column {
                    column_id: 1
                    table_name: "table_1"
                    name: "column_1"
                    type {
                    type_kind: TYPE_INT32
                    }
                }
                is_correlated: false
                }
            }
            }
            group_by_list {
            column {
                column_id: 5
                table_name: "$groupby"
                name: "column_2"
                type {
                type_kind: TYPE_STRING
                }
            }
            expr {
                resolved_column_ref_node {
                parent {
                    type {
                    type_kind: TYPE_STRING
                    }
                    type_annotation_map {
                    }
                }
                column {
                    column_id: 2
                    table_name: "table_1"
                    name: "column_2"
                    type {
                    type_kind: TYPE_STRING
                    }
                }
                is_correlated: false
                }
            }
            }
            aggregate_list {
            column {
                column_id: 3
                table_name: "$aggregate"
                name: "$agg1"
                type {
                type_kind: TYPE_INT64
                }
            }
            expr {
                resolved_function_call_base_node {
                resolved_non_scalar_function_call_base_node {
                    resolved_aggregate_function_call_node {
                    parent {
                        parent {
                        parent {
                            type {
                            type_kind: TYPE_INT64
                            }
                            type_annotation_map {
                            }
                        }
                        function {
                            name: "ZetaSQL:$count_star"
                        }
                        signature {
                            return_type {
                            kind: ARG_TYPE_FIXED
                            type {
                                type_kind: TYPE_INT64
                            }
                            options {
                                cardinality: REQUIRED
                                extra_relation_input_columns_allowed: true
                            }
                            num_occurrences: 1
                            }
                            context_id: 57
                            options {
                            is_deprecated: false
                            }
                        }
                        error_mode: DEFAULT_ERROR_MODE
                        }
                        distinct: false
                        null_handling_modifier: DEFAULT_NULL_HANDLING
                    }
                    function_call_info {
                    }
                    }
                }
                }
            }
            }
        }
        grouping_sets_column_list {
            parent {
            type {
                type_kind: TYPE_INT32
            }
            type_annotation_map {
            }
            }
            column {
            column_id: 4
            table_name: "$groupby"
            name: "column_1"
            type {
                type_kind: TYPE_INT32
            }
            }
            is_correlated: false
        }
        grouping_sets_column_list {
            parent {
            type {
                type_kind: TYPE_STRING
            }
            type_annotation_map {
            }
            }
            column {
            column_id: 5
            table_name: "$groupby"
            name: "column_2"
            type {
                type_kind: TYPE_STRING
            }
            }
            is_correlated: false
        }
        })pb",
      &expectedAggregateScanBaseNode));
  EXPECT_THAT(responseAggregateScanBaseNode, EqualsProto(expectedAggregateScanBaseNode));
}

}  // namespace local_service
}  // namespace zetasql
