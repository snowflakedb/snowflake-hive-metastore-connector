mvn install -DskipTests=true --batch-mode --show-version

PARAMS=()
PARAMS+=("-DghActionsIT")
# testing will not need shade dep. otherwise codecov cannot work
PARAMS+=("-Dnot-shadeDep")
echo "JACOCO_COVERAGE=$JACOCO_COVERAGE"
[[ -n "$JACOCO_COVERAGE" ]] && PARAMS+=("-Djacoco.skip.instrument=false")
# verify phase is after test/integration-test phase, which means both unit test
# and integration test will be run
mvn "${PARAMS[@]}" verify --batch-mode

echo ${PWD}
chmod 755 ./whitesource/run_whitesource.sh
./whitesource/run_whitesource.sh 

# Copy over the coverage report before we clean up
cp target/jacoco-ut/jacoco.xml .

mvn clean # Cleanup

