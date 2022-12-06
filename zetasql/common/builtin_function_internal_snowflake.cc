#include <ctype.h>

#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/timestamp.pb.h"
#include "google/protobuf/wrappers.pb.h"
#include "google/type/date.pb.h"
#include "google/type/timeofday.pb.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/common/builtin_function_internal.h"
#include "zetasql/common/errors.h"
#include "zetasql/proto/anon_output_with_report.pb.h"
#include "zetasql/public/anon_function.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/cycle_detector.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/proto_util.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

class AnalyzerOptions;

void GetSnowflakeAggregateFunctions(TypeFactory* type_factory,
                                    const ZetaSQLBuiltinFunctionOptions& options,
                                    NameToFunctionMap* functions) {
    const Type* int32_type = type_factory->get_int32();
    const Type* int64_type = type_factory->get_int64();
    const Type* uint32_type = type_factory->get_uint32();
    const Type* uint64_type = type_factory->get_uint64();
    const Type* double_type = type_factory->get_double();
    const Type* string_type = type_factory->get_string();
    const Type* bytes_type = type_factory->get_bytes();
    const Type* bool_type = type_factory->get_bool();
    const Type* numeric_type = type_factory->get_numeric();
    const Type* bignumeric_type = type_factory->get_bignumeric();
    const Type* interval_type = type_factory->get_interval();

    FunctionSignatureOptions has_all_integer_casting_arguments;
    has_all_integer_casting_arguments.set_constraints(&HasAllIntegerCastingArguments);

    const Function::Mode AGGREGATE = Function::AGGREGATE;

    // BITXOR( <expr1> , <expr2> )
    //   <expr1> This expression must evaluate to a data type that can be cast to INTEGER.
    //   <expr2> This expression must evaluate to a data type that can be cast to INTEGER.
    InsertFunction(functions, options, "bitxor", AGGREGATE,
                 {{ARG_TYPE_ANY_1,
                  {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1},
                  FN_BIT_XOR_INTEGER,
                  has_all_integer_casting_arguments}},
                 DefaultAggregateFunctionOptions());
}

}  // namespace zetasql
