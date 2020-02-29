#!/bin/bash

mvn clean deploy -Dmaven.test.skip=true -s ./scripts/settings.xml
