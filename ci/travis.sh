#!/usr/bin/env bash

if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
    echo "Building PR #$TRAVIS_PULL_REQUEST [$TRAVIS_PULL_REQUEST_SLUG/$TRAVIS_PULL_REQUEST_BRANCH => $TRAVIS_REPO_SLUG/$TRAVIS_BRANCH]"
elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" == "" ] ; then
    echo "Building branch $TRAVIS_REPO_SLUG/$TRAVIS_BRANCH"
elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" != "" ] ; then
    echo "Building Tag $TRAVIS_REPO_SLUG/$TRAVIS_TAG"
else
    echo "Building $TRAVIS_REPO_SLUG/$TRAVIS_BRANCH"
fi

 ./gradlew clean build -Pbranch="$TRAVIS_BRANCH"