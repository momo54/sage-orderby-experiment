#!/bin/sh

function start_sage {
  if [[ -z "$(lsof -t -i:8080)" ]]
  then
    echo "Running the SaGe server..."
    cd sage-engine
    nohup poetry run sage ../config/sage.yaml -w 8 -p 8080 > /dev/null 2>&1 &
    sleep 10
  else
    echo "The SaGe server is already running..."
  fi
}

function stop_sage {
  if [[ -z "$(lsof -t -i:8080)" ]]
  then
    echo "The SaGe server isn't running..."
  else
    echo "Stopping the SaGe server..."
    kill $(lsof -t -i:8080) 2>/dev/null
  fi
}

function start_virtuoso {
  if [[ -z "$(lsof -t -i:8890)" ]]
  then
    echo "Running the Virtuoso server..."
    nohup virtuoso-t +configfile config/virtuoso.ini +foreground > /dev/null 2>&1 &
    sleep 10
  else
    echo "The Virtuoso server is already running..."
  fi
}

function stop_virtuoso {
  if [[ -z "$(lsof -t -i:8890)" ]]
  then
    echo "The Virtuoso server isn't running..."
  else
    echo "Stopping the Virtuoso server..."
    kill $(lsof -t -i:8890) 2>/dev/null
  fi
}

if [[ "$1" == "stop" || $# -eq 1 ]]
then
  if [[ "$2" == "sage" || "$2" == "all" ]]
  then
    stop_sage
  fi
  if [[ "$2" == "virtuoso" || "$2" == "all" ]]
  then
    stop_virtuoso
  fi
fi

if [[ "$1" == "start" || $# -eq 1 ]]
then
  if [[ "$2" == "sage" || "$2" == "all" ]]
  then
    start_sage
  fi
  if [[ "$2" == "virtuoso" || "$2" == "all" ]]
  then
    start_virtuoso
  fi
fi