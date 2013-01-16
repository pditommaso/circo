#!/bin/bash
set -e
set -u
set -o errexit

#
# CLASSPATH definition 
#
CLASSPATH="./out/production/rush"
CLASSPATH="${CLASSPATH}:./out/test/rush"
for file in ./build/stage/lib/*.jar; do
  CLASSPATH="${CLASSPATH}:$file";
done

# Variable definition  
JVM_ARGS="-Djava.awt.headless=true" 
DEBUG=''
declare -a args=()

# Handle special program arguments 
while [ "$*" != "" ]; do
  if [[ "$1" == '--debug' || "$1" == '--trace' ]]; then
    DEBUG='-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8010'
    args+=("$1")

  elif [ "$1" == '--with-jrebel' ]; then  
    JVM_ARGS="$JVM_ARGS -javaagent:$JREBEL_HOME/jrebel.jar -Drebel.log.file=./jrebel-client.log"

  else
   args+=("$1")
  fi 
  # move to the next
  shift
done

# Show some variable when in DEBUG mode
if [ "$DEBUG" != '' ]; then
  echo Launch environment
  echo ------------------
  echo jvmargs: $JVM_ARGS
  echo debug: $DEBUG  
  echo classpath: 
  echo $CLASSPATH | tr ":" "\n" | sort
  echo ''
  echo Launching it!
  echo ------------------
fi

# Launch the APP
exec java $JVM_ARGS $DEBUG -cp "$CLASSPATH" "${args[@]}"