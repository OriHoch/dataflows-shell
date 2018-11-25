FROM python:3.7.1
COPY VERSION.txt .
RUN python3 -m pip install 'dataflows-shell>='$(cat VERSION.txt)
RUN mkdir /workdir
WORKDIR /workdir
ENTRYPOINT ["dfs"]
