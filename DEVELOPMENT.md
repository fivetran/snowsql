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

### Build needed package
```
bazel build //zetasql/analyzer:all --features=-supports_dynamic_linker -c dbg --spawn_strategy=local
```
or
```
bazel build //zetasql/analyzer:resolver_test --features=-supports_dynamic_linker -c dbg --spawn_strategy=local
```
### Add command to `Execute test` configuration in `launch.json`
```
./bazel-bin/zetasql/analyzer/analyzer_aggregation_test --test_file=./zetasql/analyzer/testdata/aggregation.test
```
`analyzer_aggregation_test` = `<package_name>` + `'_'` + `<test_name>`<br>
`<test_name>` = file name from `'zetasql/<package_name>/testdata'` without `'.test'` extension (see `'gen_analyzer_test'` in `'zetasql/analyzer/BUILD'`).
