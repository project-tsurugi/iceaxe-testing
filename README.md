# iceaxe-testing - Iceaxe test

[![javadoc](https://javadoc.io/badge2/com.tsurugidb.iceaxe/iceaxe-core/javadoc.svg)](https://javadoc.io/doc/com.tsurugidb.iceaxe/iceaxe-core)

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

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

