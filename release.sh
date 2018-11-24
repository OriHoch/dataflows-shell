#!/usr/bin/env bash

VERSION_LABEL="${1}"

[ "${VERSION_LABEL}" == "" ] \
    && echo Missing version label \
    && echo current VERSION.txt = $(cat VERSION.txt) \
    && exit 1

echo "${VERSION_LABEL}" > VERSION.txt &&\
python setup.py sdist &&\
twine upload dist/dataflows_shell-${VERSION_LABEL}.tar.gz &&\
echo dataflows_shell-${VERSION_LABEL} &&\
echo Great Success &&\
exit 0

exit 1
