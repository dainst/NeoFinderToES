#!/bin/sh
echo "Wrapping jar file..."
cat jarwrapper/base.sh target/NeoFinderToES-1.0-SNAPSHOT-jar-with-dependencies.jar > target/neofindertoes && chmod +x target/neofindertoes
