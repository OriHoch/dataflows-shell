#!/usr/bin/env bash

VERSION_LABEL="${1}"

[ "${VERSION_LABEL}" == "" ] \
    && echo Missing version label \
    && echo current VERSION.txt = $(cat VERSION.txt) \
    && exit 1

echo "${VERSION_LABEL}" > VERSION.txt &&\
python setup.py sdist &&\
twine upload dist/dataflows_shell-${VERSION_LABEL}.tar.gz &&\
while ! docker build -t orihoch/dataflows-shell:v${VERSION_LABEL} .; do sleep 2; done &&\
docker push orihoch/dataflows-shell:v${VERSION_LABEL} &&\
docker tag orihoch/dataflows-shell:v${VERSION_LABEL} orihoch/dataflows-shell:latest &&\
docker push orihoch/dataflows-shell:latest &&\
echo orihoch/dataflows-shell:v${VERSION_LABEL} &&\
echo orihoch/dataflows-shell:latest &&\
echo Great Success &&\
exit 0

exit 1
