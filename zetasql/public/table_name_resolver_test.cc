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

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/btree_set.h"

namespace zetasql {

using ::testing::ContainerEq;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;
using ::zetasql_base::testing::StatusIs;

// Note: Most test coverage of TableNameResolver comes through the regular
// analyzer tests, which automatically compare the result of FindTables() with
// the resolved AST. Scripting statements, however, cannot be tested that way,
// since they are not supported by the resolver, so we cover them here.

TEST(TableNameResolver, ScriptStatementsWithFindTables) {
  std::string script = R"(
  -- DECLARE statements
  DECLARE a,b INT64;
  DECLARE c INT64 DEFAULT (SELECT SUM(x) FROM t1);
  DECLARE d, e DEFAULT (SELECT SUM(x) FROM t2);

  -- SET statements
  SET a = (SELECT SUM(x) FROM t3 FOR SYSTEM TIME AS of '2020-01-01');
  SET (b, c) = (SELECT x FROM t4 AS t4_alias);
  SET @@d = (SELECT SUM(x) FROM t5, t6);
  RAISE USING MESSAGE = (SELECT x FROM t7);
  RAISE;
  )";

  // Expected output as map(stmt index=>{tables, resolution_info}).
  absl::flat_hash_map<int, TableNamesSet> expected_table_refs;
  expected_table_refs[0] = {};
  expected_table_refs[1] = {{"t1"}};
  expected_table_refs[2] = {{"t2"}};
  expected_table_refs[3] = {{"t3"}};
  expected_table_refs[4] = {{"t4"}};
  expected_table_refs[5] = {{"t5"}, {"t6"}};
  expected_table_refs[6] = {{"t7"}};
  expected_table_refs[7] = {};

  // Parse the script and compare actual table references against expected.
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_ASSERT_OK(ParseScript(script, ParserOptions(),
                        ERROR_MESSAGE_MULTI_LINE_WITH_CARET, &parser_output));

  AnalyzerOptions analyzer_options;
  analyzer_options.CreateDefaultArenasIfNotSet();
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_ASSIGNMENT_STMT);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_V_1_1_FOR_SYSTEM_TIME_AS_OF);
  TableNamesSet tables;
  ASSERT_EQ(expected_table_refs.size(),
            parser_output->script()->statement_list().size());
  int idx = 0;
  for (const ASTStatement* stmt : parser_output->script()->statement_list()) {
    // Try FindTables()
    ZETASQL_ASSERT_OK(ExtractTableNamesFromASTStatement(*stmt, analyzer_options, script,
                                                &tables));
    ASSERT_EQ(expected_table_refs[idx], tables);
    ++idx;
  }
}

TEST(TableNameResolver, ScriptStatementWithTimeTravel) {
  std::string script = R"(
  SET a = (SELECT SUM(x) FROM t3 FOR SYSTEM TIME AS of '2020-01-01');
  )";

  // Expected output as map(stmt index=>{tables, resolution_info}).
  TableNamesSet expected_table_refs = {{"t3"}};

  // Parse the script and compare actual table references against expected.
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_ASSERT_OK(ParseScript(script, ParserOptions(),
                        ERROR_MESSAGE_MULTI_LINE_WITH_CARET, &parser_output));

  AnalyzerOptions analyzer_options;
  analyzer_options.CreateDefaultArenasIfNotSet();
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_ASSIGNMENT_STMT);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_V_1_1_FOR_SYSTEM_TIME_AS_OF);
  TableNamesSet tables;
  ASSERT_EQ(expected_table_refs.size(),
            parser_output->script()->statement_list().size());
  for (const ASTStatement* stmt : parser_output->script()->statement_list()) {
    TypeFactory type_factory;
    SimpleCatalog empty_catalog("empty_catalog", &type_factory);
    TableResolutionTimeInfoMap time_info_map;
    ZETASQL_ASSERT_OK(ExtractTableNamesFromASTStatement(*stmt, analyzer_options,
                                               script, &tables));
    ZETASQL_ASSERT_OK(ExtractTableResolutionTimeFromASTStatement(
        *stmt, analyzer_options, script, &type_factory, &empty_catalog,
        &time_info_map));
    ASSERT_EQ(expected_table_refs, tables);
    ASSERT_EQ(tables.size(), time_info_map.size());
    for (const auto& table : tables) {
      auto it = time_info_map.find(table);
      ASSERT_EQ(false, it == time_info_map.end());
      ASSERT_FALSE(it->second.has_default_resolution_time);
      ASSERT_EQ(1, it->second.exprs.size());
      const ResolvedExpr* resolved_expr =
          it->second.exprs[0].analyzer_output_with_expr->resolved_expr();
      ASSERT_EQ(resolved_expr->node_kind(), RESOLVED_LITERAL);
      ASSERT_EQ("'2020-01-01'",
                resolved_expr->GetAs<ResolvedLiteral>()->value().DebugString());
    }
  }
}

TEST(TableNameResolver, UnsupportedAssignmentStatements) {
  std::string script = R"(
  SET a = (SELECT SUM(x) FROM t3 FOR SYSTEM TIME AS of '2020-01-01');
  SET (b, c) = (SELECT x FROM t4 AS t4_alias);
  SET @@d = (SELECT SUM(x) FROM t5);
  )";

  // Parse the script and compare actual table references against expected.
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_ASSERT_OK(ParseScript(script, ParserOptions(),
                        ERROR_MESSAGE_MULTI_LINE_WITH_CARET, &parser_output));

  AnalyzerOptions analyzer_options;
  analyzer_options.CreateDefaultArenasIfNotSet();
  TableNamesSet tables;
  for (const ASTStatement* stmt : parser_output->script()->statement_list()) {
    ASSERT_THAT(ExtractTableNamesFromASTStatement(*stmt, analyzer_options,
                                                  script, &tables),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("Statement not supported")));
  }
}

TEST(TableNameResolver, ExtractTableNamesFromStatement) {
  std::set<std::vector<std::string>> table_names;
  std::set<std::vector<std::string>> tvf_names;
  std::string sql = R"(SELECT * FROM foo(bar), sometable.x)";

  // Extract table names without extracting tvf info
  ZETASQL_ASSERT_OK(
      ExtractTableNamesFromStatement(sql, AnalyzerOptions(), &table_names));
  EXPECT_THAT(table_names, UnorderedElementsAre(
                               std::vector<std::string>({"sometable", "x"})));

  // Extract table names and tvf info
  ZETASQL_ASSERT_OK(ExtractTableNamesFromStatement(sql, AnalyzerOptions(), &table_names,
                                           &tvf_names));
  EXPECT_THAT(tvf_names, UnorderedElementsAre(
                             ContainerEq(std::vector<std::string>{"foo"})));
}

TEST(TableNameResolver, ExtractTableNamesFromStatementMultiple) {
  std::set<std::vector<std::string>> table_names;
  std::set<std::vector<std::string>> tvf_names;
  std::string sql = R"(SELECT * FROM foo(f(x),y,12), a.b(c))";

  // Extract table names and tvf info in a case with multiple TVF
  ZETASQL_ASSERT_OK(ExtractTableNamesFromStatement(sql, AnalyzerOptions(), &table_names,
                                           &tvf_names));
  EXPECT_THAT(tvf_names,
              UnorderedElementsAre(ElementsAre("foo"), ElementsAre("a", "b")));
}

TEST(TableNameResolver, ExtractTableNamesFromNextStatement) {
  absl::btree_set<std::vector<std::string>> table_names;
  absl::btree_set<std::vector<std::string>> tvf_names;
  std::string sql =
      R"(SELECT * FROM foo(bar);
      SELECT * FROM f(x), g(x); SELECT * FROM a(1), b(2), c(3))";

  // Extract TVF names from multiple statements
  std::vector<std::set<std::vector<std::string>>> actual;
  zetasql::ParseResumeLocation parse_location =
      zetasql::ParseResumeLocation::FromStringView(sql);
  bool at_end_of_input = false;
  while (!at_end_of_input) {
    std::set<std::vector<std::string>> tvf_names;
    zetasql::TableNamesSet table_names_from_statement;
    ZETASQL_ASSERT_OK(zetasql::ExtractTableNamesFromNextStatement(
        &parse_location, AnalyzerOptions(), &table_names_from_statement,
        &at_end_of_input, &tvf_names));
    actual.push_back(tvf_names);
  }
  ASSERT_THAT(table_names, IsEmpty());
  EXPECT_THAT(
      actual,
      ElementsAre(UnorderedElementsAre(ElementsAre("foo")),
                  UnorderedElementsAre(ElementsAre("f"), ElementsAre("g")),
                  UnorderedElementsAre(ElementsAre("a"), ElementsAre("b"),
                                       ElementsAre("c"))));
}

TEST(TableNameResolver, ExtractTableNamesFromASTStatement) {
  std::set<std::vector<std::string>> table_names;
  absl::btree_set<std::vector<std::string>> tvf_names;
  std::string sql =
      R"(SELECT * FROM foo(bar);
      SELECT * FROM f(x), g(x); SELECT * FROM a(1), b(2), c(3))";

  // Extract TVF names from AST statement
  std::unique_ptr<zetasql::ParserOutput> parser_output;
  ZETASQL_ASSERT_OK(zetasql::ParseScript(
      sql, zetasql::ParserOptions(),
      zetasql::ERROR_MESSAGE_MULTI_LINE_WITH_CARET, &parser_output));

  std::vector<std::set<std::vector<std::string>>> actual;
  zetasql::ParseResumeLocation parse_location =
      zetasql::ParseResumeLocation::FromStringView(sql);
  for (const zetasql::ASTStatement* stmt :
       parser_output->script()->statement_list()) {
    std::set<std::vector<std::string>> tvf_names;
    ZETASQL_ASSERT_OK(zetasql::ExtractTableNamesFromASTStatement(
        *stmt, AnalyzerOptions(), "", &table_names, &tvf_names));
    actual.push_back(tvf_names);
  }
  ASSERT_THAT(table_names, IsEmpty());
  EXPECT_THAT(
      actual,
      ElementsAre(UnorderedElementsAre(ElementsAre("foo")),
                  UnorderedElementsAre(ElementsAre("f"), ElementsAre("g")),
                  UnorderedElementsAre(ElementsAre("a"), ElementsAre("b"),
                                       ElementsAre("c"))));
}

TEST(TableNameResolver, ExtractTableNamesFromScript) {
  std::set<std::vector<std::string>> table_names;
  std::set<std::vector<std::string>> tvf_names;
  std::string sql =
      R"(SELECT * FROM foo(bar);
      SELECT * FROM f(x), g(x);
      SELECT a FROM T1.T2(y) as a)";

  // Extract TVF names from a script
  ZETASQL_ASSERT_OK(ExtractTableNamesFromScript(sql, AnalyzerOptions(), &table_names,
                                        &tvf_names));
  ASSERT_THAT(table_names, IsEmpty());
  EXPECT_THAT(tvf_names,
              UnorderedElementsAre(ElementsAre("foo"), ElementsAre("f"),
                                   ElementsAre("g"), ElementsAre("T1", "T2")));
}

TEST(TableNameResolver, ExtractTableNamesFromScriptWithSubquery) {
  std::set<std::vector<std::string>> table_names;
  std::set<std::vector<std::string>> tvf_names;
  std::string sql = R"r(
    WITH
      my_cte AS (
        SELECT x
        FROM table_name
      )
    SELECT x
    FROM my_cte
    WHERE (SELECT 1 FROM my_cte)
  )r";

  ZETASQL_ASSERT_OK(ExtractTableNamesFromScript(sql, AnalyzerOptions(), &table_names,
                                        &tvf_names));
  EXPECT_THAT(table_names, UnorderedElementsAre(ElementsAre("table_name")));
  ASSERT_THAT(tvf_names, IsEmpty());
}

TEST(TableNameResolver, ExtractTableNamesFromASTScript) {
  std::set<std::vector<std::string>> table_names;
  std::set<std::vector<std::string>> tvf_names;
  std::string sql =
      R"(SELECT * FROM foo(bar);
      SELECT * FROM f(x), g(x);
      SELECT col1 from T;)";

  // Extract TVF names from AST script
  std::unique_ptr<zetasql::ParserOutput> parser_output;
  ZETASQL_ASSERT_OK(zetasql::ParseScript(
      sql, zetasql::ParserOptions(),
      zetasql::ERROR_MESSAGE_MULTI_LINE_WITH_CARET, &parser_output));

  const zetasql::ASTScript* script = parser_output->script();
  ZETASQL_ASSERT_OK(zetasql::ExtractTableNamesFromASTScript(
      *script, AnalyzerOptions(), sql, &table_names, &tvf_names));
  EXPECT_THAT(table_names, UnorderedElementsAre(ElementsAre("T")));
  EXPECT_THAT(tvf_names,
              UnorderedElementsAre(ElementsAre("foo"), ElementsAre("f"),
                                   ElementsAre("g")));
}

}  // namespace zetasql
