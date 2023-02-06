#ifndef ZETASQL_PUBLIC_TYPES_VARIANT_TYPE_H_
#define ZETASQL_PUBLIC_TYPES_VARIANT_TYPE_H_

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/container_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/base/case.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"

namespace zetasql {

class LanguageOptions;
class TypeFactory;
class TypeParameterValue;
class TypeParameters;
class ValueContent;
class ValueProto;
class TypeModifiers;

// A variant type.
// Variants are allowed to have no internal type.
class VariantType : public ContainerType {
 public:
#ifndef SWIG
  VariantType(const VariantType&) = delete;
  VariantType& operator=(const VariantType&) = delete;
#endif  // SWIG

  // The element type of the variant.
  const Type* element_type() const { return element_type_; }

  bool UsingFeatureV12CivilTimeType() const override {
    return element_type_->UsingFeatureV12CivilTimeType();
  }

  const VariantType* AsVariant() const override { return this; }

  std::string ShortTypeName(ProductMode mode) const override;
  std::string TypeName(ProductMode mode) const override;

  // Same as above, but the type modifier values are appended to the SQL name
  // for this VariantType.
  absl::StatusOr<std::string> TypeNameWithModifiers(
      const TypeModifiers& type_modifiers, ProductMode mode) const override;

  bool IsSupportedType(const LanguageOptions& language_options) const override;

  int nesting_depth() const override {
    return element_type_->nesting_depth() + 1;
  }

  // Helper function for determining if a type is a valid variant element type.
  static bool IsValidElementType(const Type* element_type);

 protected:
  std::string GetFormatPrefix(
      const ValueContent& value_content,
      const Type::FormatValueContentOptions& options) const override {
    if (options.mode == Type::FormatValueContentOptions::Mode::kDebug) {
      return "Variant(";
    }
    return absl::StrCat(TypeName(options.product_mode), "[");
  }

  char GetFormatClosingCharacter(
      const Type::FormatValueContentOptions& options) const override {
    return ')';
  }

  const Type* GetElementType(int index) const override;

  std::string GetFormatElementPrefix(
      const int index, const bool is_null,
      const FormatValueContentOptions& options) const override {
    return "";
  }

 private:
  VariantType(const TypeFactory* factory, const Type* element_type);
  ~VariantType() override;

  // Helper function for determining if a type kind is a supported variant element
  // type kind.
  static bool IsSupportedElementTypeKind(const TypeKind element_type_kind);

  // Helper function for determining equality or equivalence for variant types.
  // Equals means that the variant element type is the same.
  static bool EqualsImpl(const VariantType* type1, const VariantType* type2,
                         bool equivalent);

  bool SupportsGroupingImpl(const LanguageOptions& language_options,
                            const Type** no_grouping_type) const override {
    if (no_grouping_type != nullptr) {
      *no_grouping_type = nullptr;
    }
    return true;
  }

  absl::Status SerializeToProtoAndDistinctFileDescriptorsImpl(
      const BuildFileDescriptorSetMapOptions& options, TypeProto* type_proto,
      FileDescriptorSetMap* file_descriptor_set_map) const override;

  bool EqualsForSameKind(const Type* that, bool equivalent) const override;

  void DebugStringImpl(bool details, TypeOrStringVector* stack,
                       std::string* debug_string) const override;

  // Return estimated size of memory owned by this type. Variant's owned memory
  // does not include its element type memory (which is owned by some
  // TypeFactory).
  int64_t GetEstimatedOwnedMemoryBytesSize() const override {
    return sizeof(*this);
  }

  void CopyValueContent(const ValueContent& from,
                        ValueContent* to) const override;
  void ClearValueContent(const ValueContent& value) const override;

  absl::HashState HashTypeParameter(absl::HashState state) const override;
  absl::HashState HashValueContent(const ValueContent& value,
                                   absl::HashState state) const override;
  std::string FormatValueContentContainerElement(
      const internal::ValueContentContainerElement& element,
      const Type::FormatValueContentOptions& options) const;
  std::string FormatValueContent(
      const ValueContent& value,
      const FormatValueContentOptions& options) const override;
  bool ValueContentEquals(
      const ValueContent& x, const ValueContent& y,
      const ValueEqualityCheckOptions& options) const override;
  bool ValueContentLess(const ValueContent& x, const ValueContent& y,
                        const Type* other_type) const override;
  absl::Status SerializeValueContent(const ValueContent& value,
                                     ValueProto* value_proto) const override;
  absl::Status DeserializeValueContent(const ValueProto& value_proto,
                                       ValueContent* value) const override;

  const Type* const element_type_;

  friend class TypeFactory;
  // friend class VariantTypeTestPeer;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPES_VARIANT_TYPE_H_
