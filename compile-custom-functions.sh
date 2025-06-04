#!/bin/bash
# shellcheck disable=SC2155

function cleanupTargetDir() {
    rm -rf ./custom-functions/target
}

function main() {
    cleanupTargetDir

    javac -cp "$GEMFIRE_HOME/lib/*" \
      -d ./custom-functions/target/classes \
      custom-functions/src/com/broadcom/functions/*.java

    if [ $? -ne 0 ]; then
        echo "Compilation failed"
        exit 1
    fi

    pushd ./custom-functions/target/classes || exit 1
    jar cf ../custom-functions.jar *.class
    popd || exit 1
}

main
