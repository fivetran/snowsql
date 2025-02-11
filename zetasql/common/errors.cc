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

#include "zetasql/common/errors.h"

#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/error_location.pb.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// Returns true if <status> has a internalErrorLocation payload.
static bool HasInternalErrorLocation(const absl::Status& status) {
  return internal::HasPayloadWithType<InternalErrorLocation>(status);
}

absl::Status StatusWithInternalErrorLocation(
    const absl::Status& status, const ParseLocationPoint& error_location) {
  if (status.ok()) return status;

  absl::Status result = status;
  internal::AttachPayload(&result, error_location.ToInternalErrorLocation());
  return result;
}

ErrorSource MakeErrorSource(const absl::Status& status, std::string_view text,
                            ErrorMessageMode mode) {
  ABSL_DCHECK(!status.ok());
  // Sanity check that status does not have an InternalErrorLocation.
  ABSL_DCHECK(!HasInternalErrorLocation(status));

  ErrorSource error_source;
  error_source.set_error_message(std::string(status.message()));
  ErrorLocation status_error_location;
  if (GetErrorLocation(status, &status_error_location)) {
    *error_source.mutable_error_location() = status_error_location;
    if (mode == ErrorMessageMode::ERROR_MESSAGE_MULTI_LINE_WITH_CARET &&
        !text.empty()) {
      error_source.set_error_message_caret_string(
          GetErrorStringWithCaret(text, status_error_location));
    }
  }
  return error_source;
}

// Returns ErrorSources from <status>, if present.
const std::optional<::google::protobuf::RepeatedPtrField<ErrorSource>> GetErrorSources(
    const absl::Status& status) {
  if (internal::HasPayloadWithType<ErrorLocation>(status)) {
    // Sanity check that an OK status does not have a payload.
    ABSL_DCHECK(!status.ok());

    return internal::GetPayload<ErrorLocation>(status).error_source();
  }
  return std::nullopt;
}

std::string DeprecationWarningsToDebugString(
    const std::vector<FreestandingDeprecationWarning>& warnings) {
  if (warnings.empty()) return "";
  return absl::StrCat("(", warnings.size(), " deprecation warning",
                      (warnings.size() > 1 ? "s" : ""), ")");
}

absl::StatusOr<FreestandingDeprecationWarning> StatusToDeprecationWarning(
    const absl::Status& from_status, absl::string_view sql) {
  ZETASQL_RET_CHECK(absl::IsInvalidArgument(from_status))
      << "Deprecation statuses must have code INVALID_ARGUMENT";

  FreestandingDeprecationWarning warning;
  warning.set_message(std::string(from_status.message()));

  ZETASQL_RET_CHECK(internal::HasPayload(from_status))
      << "Deprecation statuses must have payloads";

  ZETASQL_RET_CHECK(!internal::HasPayloadWithType<InternalErrorLocation>(from_status))
      << "Deprecation statuses cannot have InternalErrorLocation payloads";

  ZETASQL_RET_CHECK(internal::HasPayloadWithType<ErrorLocation>(from_status))
      << "Deprecation statuses must have ErrorLocation payloads";
  *warning.mutable_error_location() =
      internal::GetPayload<ErrorLocation>(from_status);

  ZETASQL_RET_CHECK(internal::HasPayloadWithType<DeprecationWarning>(from_status))
      << "Deprecation statuses must have DeprecationWarning payloads";
  *warning.mutable_deprecation_warning() =
      internal::GetPayload<DeprecationWarning>(from_status);

  ZETASQL_RET_CHECK_EQ(internal::GetPayloadCount(from_status), 2)
      << "Found invalid extra payload in deprecation status";

  warning.set_caret_string(
      GetErrorStringWithCaret(sql, warning.error_location()));

  return warning;
}

absl::StatusOr<std::vector<FreestandingDeprecationWarning>>
StatusesToDeprecationWarnings(const std::vector<absl::Status>& from_statuses,
                              absl::string_view sql) {
  std::vector<FreestandingDeprecationWarning> warnings;
  for (const absl::Status& from_status : from_statuses) {
    ZETASQL_ASSIGN_OR_RETURN(const FreestandingDeprecationWarning warning,
                     StatusToDeprecationWarning(from_status, sql));
    warnings.emplace_back(warning);
  }

  return warnings;
}

}  // namespace zetasql
