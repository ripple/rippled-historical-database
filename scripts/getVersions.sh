#!/bin/bash
set -e

if hash rpm 2>/dev/null;
then
  rpm -Uvh https://mirrors.ripple.com/ripple-repo-el7.rpm >/dev/null

  RIPPLE_REPO="nightly"
  yum --disablerepo=* --enablerepo=ripple-$RIPPLE_REPO clean expire-cache >/dev/null
  NIGHTLY="$(repoquery --enablerepo=ripple-$RIPPLE_REPO --releasever=el7 --qf="%{version}" rippled | tr _ -)"
  RIPPLE_REPO="stable"
  yum --disablerepo=* --enablerepo=ripple-$RIPPLE_REPO clean expire-cache >/dev/null
  STABLE="$(repoquery --enablerepo=ripple-$RIPPLE_REPO --releasever=el7 --qf="%{version}" rippled | tr _ -)"
  RIPPLE_REPO="unstable"
  yum --disablerepo=* --enablerepo=ripple-$RIPPLE_REPO clean expire-cache >/dev/null
  UNSTABLE="$(repoquery --enablerepo=ripple-$RIPPLE_REPO --releasever=el7 --qf="%{version}" rippled | tr _ -)"

  echo "{\"stable\":\"$STABLE\",\"unstable\":\"$UNSTABLE\",\"nightly\":\"$NIGHTLY\"}"
  exit 0
else
  echo "yum not installed"
  exit 1
fi

