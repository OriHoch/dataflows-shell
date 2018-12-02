FROM python:3.7.1

RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl &&\
    chmod +x ./kubectl && mv ./kubectl /usr/local/bin/kubectl

COPY . /usr/local/src/dataflows-shell/
RUN python3 -m pip install -e /usr/local/src/dataflows-shell/
RUN mkdir /workdir
WORKDIR /workdir
ENV DFS_FEATURE_ENABLE_ALL=1
ENTRYPOINT ["dfs"]
