
reproduce a bug of Impala JDBC Driver 2.6.17.

### Package

```
make build
```

### Test

```
java -jar impala-jdbc-sql-rewrite-1.0-SNAPSHOT.jar 1.sql
java -jar impala-jdbc-sql-rewrite-1.0-SNAPSHOT.jar 2.sql
```