#!/bin/bash
set -e

if hash apt-get 2>/dev/null;
then
  apt-get update
  apt-get -y install sudo
  apt-get -y install yum-utils
  echo "installation finished"
else
 echo "yum-utils not installed"
fi
