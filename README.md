# iceaxe-testing - Iceaxe test

## Requirements

* Java `>= 11`

* access to installed dependent modules:
  * iceaxe-core
  * Tsubakuro

## How to execute

### Execute

```bash
cd iceaxe-testing
./gradlew test
```

### Execute with iceaxe-core, Tsubakuro that installed locally

Execute with Gradle Property `mavenLocal` .

```bash
cd iceaxe-testing
./gradlew test -PmavenLocal
```

### Execute with endpoint

```bash
./gradlew test -Pdbtest.endpoint=tcp://localhost:12345
```

### Execute with credential

```bash
./graglew test \
-Pdbtest.user=user \
-Pdbtest.password=password \
-Pdbtest.auth-token=token \
-Pdbtest.credentials=/path/to/credential-file
```

For tests other than credential, specifying only one of `user`, `auth-token`, or `credentials` is sufficient. If none of these are specified, authentication will be performed using the user `tsurugi`.

In the credential test, anything not specified is skipped.

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

