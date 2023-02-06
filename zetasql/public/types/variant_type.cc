#include "zetasql/public/types/variant_type.h"

#include <string>
#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/collation.h"
#include "zetasql/public/types/container_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/types/type_modifiers.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/types/value_equality_check_options.h"
#include "zetasql/public/types/value_representations.h"
#include "zetasql/public/value.pb.h"
#include "zetasql/public/value_content.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/simple_reference_counted.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

std::string VariantType::ShortTypeName(ProductMode mode) const {
  return absl::StrCat("VARIANT<", element_type_->ShortTypeName(mode), ">");
}

std::string VariantType::TypeName(ProductMode mode) const {
  return absl::StrCat("VARIANT<", element_type_->TypeName(mode), ">");
}

absl::StatusOr<std::string> VariantType::TypeNameWithModifiers(
    const TypeModifiers& type_modifiers, ProductMode mode) const {
  throw "Not implemented";
}

bool VariantType::IsSupportedType(const LanguageOptions& language_options) const {
  return IsValidElementType(element_type_) &&
         element_type_->IsSupportedType(language_options);
}

const Type* VariantType::GetElementType(int i) const { return element_type(); }

VariantType::VariantType(const TypeFactory* factory, const Type* element_type)
    : ContainerType(factory, TYPE_VARIANT), element_type_(element_type) {
  // Also blocked in TypeFactory::MakeVariantType.
  ZETASQL_DCHECK(IsValidElementType(element_type_));
}
VariantType::~VariantType() {}

bool VariantType::IsValidElementType(const Type* element_type) {
  // TODO: Implement
  return IsSupportedElementTypeKind(element_type->kind());
}

bool VariantType::IsSupportedElementTypeKind(const TypeKind element_type_kind) {
  // TODO: Implement
  return true;
}

bool VariantType::EqualsImpl(const VariantType* const type1,
                             const VariantType* const type2, bool equivalent) {
  return type1->element_type()->EqualsImpl(type2->element_type(), equivalent);
}

absl::Status VariantType::SerializeToProtoAndDistinctFileDescriptorsImpl(
    const BuildFileDescriptorSetMapOptions& options, TypeProto* type_proto,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  type_proto->set_type_kind(kind_);
  return element_type()->SerializeToProtoAndDistinctFileDescriptorsImpl(
      options, type_proto->mutable_range_type()->mutable_element_type(),
      file_descriptor_set_map);
}

bool VariantType::EqualsForSameKind(const Type* that, bool equivalent) const {
  const VariantType* other = that->AsVariant();
  ZETASQL_DCHECK(other);
  return EqualsImpl(this, other, equivalent);
}

void VariantType::DebugStringImpl(bool details, TypeOrStringVector* stack,
                                  std::string* debug_string) const {
  absl::StrAppend(debug_string, "VARIANT<");
  stack->push_back(">");
  stack->push_back(element_type());
}

void VariantType::CopyValueContent(const ValueContent& from,
                                   ValueContent* to) const {
  from.GetAs<zetasql_base::SimpleReferenceCounted*>()->Ref();
  *to = from;
}

void VariantType::ClearValueContent(const ValueContent& value) const {
  value.GetAs<zetasql_base::SimpleReferenceCounted*>()->Unref();
}

absl::HashState VariantType::HashTypeParameter(absl::HashState state) const {
  // Variant types are equivalent if their element types are equivalent,
  // so we hash the element type kind.
  return element_type()->Hash(std::move(state));
}

absl::HashState VariantType::HashValueContent(const ValueContent& value,
                                              absl::HashState state) const {
  absl::HashState result = absl::HashState::Create(&state);
  const internal::ValueContentContainer* container =
      value.GetAs<internal::ValueContentContainerRef*>()->value();
  ZETASQL_DCHECK_EQ(container->num_elements(), 2);
  ValueContentContainerElementHasher hasher(element_type());
  const internal::ValueContentContainerElement& start = container->element(0);
  result = absl::HashState::combine(std::move(result), hasher(start));
  const internal::ValueContentContainerElement& end = container->element(1);
  result = absl::HashState::combine(std::move(result), hasher(end));
  return result;
}

std::string VariantType::FormatValueContentContainerElement(
    const internal::ValueContentContainerElement& element,
    const Type::FormatValueContentOptions& options) const {
  std::string result;
  if (element.is_null()) {
    if (options.mode == Type::FormatValueContentOptions::Mode::kSQLLiteral ||
        options.mode == Type::FormatValueContentOptions::Mode::kSQLExpression) {
      return "UNBOUNDED";
    }
    result = "NULL";
  } else {
    Type::FormatValueContentOptions element_format_options = options;
    // Set mode to Debug to get elements formatted without added type prefix
    element_format_options.mode = Type::FormatValueContentOptions::Mode::kDebug;
    result = element_type()->FormatValueContent(element.value_content(),
                                                element_format_options);
  }

  if (options.mode == Type::FormatValueContentOptions::Mode::kDebug &&
      options.verbose) {
    return absl::StrCat(element_type()->CapitalizedName(), "(", result, ")");
  }
  return result;
}

std::string VariantType::FormatValueContent(
    const ValueContent& value,
    const Type::FormatValueContentOptions& options) const {
  const internal::ValueContentContainer* container =
      value.GetAs<internal::ValueContentContainerRef*>()->value();
  const internal::ValueContentContainerElement& element = container->element(0);

  std::string formatted_element = FormatValueContentContainerElement(element, options);
  if (options.mode == Type::FormatValueContentOptions::Mode::kDebug) {
    return formatted_element;
  }
  return absl::StrCat(TypeName(options.product_mode), " ",
                      ToStringLiteral(formatted_element));
}

bool VariantType::ValueContentEquals(
    const ValueContent& x, const ValueContent& y,
    const ValueEqualityCheckOptions& options) const {
  const internal::ValueContentContainer* x_container =
      x.GetAs<internal::ValueContentContainerRef*>()->value();
  const internal::ValueContentContainer* y_container =
      y.GetAs<internal::ValueContentContainerRef*>()->value();

  const internal::ValueContentContainerElement& x_element =
      x_container->element(0);
  const internal::ValueContentContainerElement& y_element =
      y_container->element(0);

  ValueContentContainerElementEq eq(options, element_type());

  return eq(x_element, y_element);
}

bool VariantType::ValueContentLess(const ValueContent& x, const ValueContent& y,
                                   const Type* other_type) const {
  const internal::ValueContentContainer* x_container =
      x.GetAs<internal::ValueContentContainerRef*>()->value();
  const internal::ValueContentContainer* y_container =
      y.GetAs<internal::ValueContentContainerRef*>()->value();

  const internal::ValueContentContainerElement& x_element =
      x_container->element(0);
  const internal::ValueContentContainerElement& y_element =
      y_container->element(0);

  const Type* x_element_type = element_type();
  const Type* y_element_type = other_type->AsVariant()->element_type();

  ValueEqualityCheckOptions options;
  ValueContentContainerElementEq eq(options, element_type());

  if (!eq(x_element, y_element)) {
    if (y_element.is_null()) {
      return false;
    }
    return x_element.is_null() ||
           ValueContentContainerElementLess(x_element, y_element, x_element_type,
                                            y_element_type)
               .value_or(false);
  }

  return false;
}

absl::Status VariantType::SerializeValueContent(const ValueContent& value,
                                                ValueProto* value_proto) const {
  return absl::FailedPreconditionError(
      "SerializeValueContent should never be called for VariantType, since its "
      "value content is maintained in the Value class");
}

absl::Status VariantType::DeserializeValueContent(const ValueProto& value_proto,
                                                ValueContent* value) const {
  return absl::FailedPreconditionError(
      "DeserializeValueContent should never be called for VariantType, since its "
      "value content deserialization is maintained in the Value class");
}

}
