build:
	mvn initialize && mvn package

package:build
	mkdir -p out && cp *.sql out && cp target/impala-jdbc-sql-rewrite-1.0-SNAPSHOT.jar out && zip -r out.zip out/
