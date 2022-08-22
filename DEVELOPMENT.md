# Preparation

## Install extensions
   - [Bazel](https://marketplace.visualstudio.com/items?itemName=BazelBuild.vscode-bazel)
   - [CodeLLDB](https://marketplace.visualstudio.com/items?itemName=vadimcn.vscode-lldb)
   - [C/C++ Extension Pack](https://marketplace.visualstudio.com/items?itemName=ms-vscode.cpptools-extension-pack)

## Build exec query tool
```
bazel build //zetasql/tools/execute_query:execute_query -c dbg --spawn_strategy=local
```

## Build one package
```
bazel build //zetasql/parser/... --features=-supports_dynamic_linker
```

## Run tests for one package
```
bazel test //zetasql/parser/... --features=-supports_dynamic_linker
```

## Build and debug tests
```
bazel build //zetasql/analyzer:all --features=-supports_dynamic_linker -c dbg --spawn_strategy=local

./bazel-bin/zetasql/analyzer/analyzer_aggregation_test --test_file=./zetasql/analyzer/testdata/aggregation.test
```
