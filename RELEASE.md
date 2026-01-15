# Release

To release a new version (after you ran the tests):

```sh
mvn -DnewVersion=4.0.0-serde-fixes versions:set
mvn deploy -DskipTests
mvn versions:commit
git commit -m "new version"
```
