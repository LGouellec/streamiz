FROM gitpod/workspace-full:latest

USER gitpod
#.NET installed via .gitpod.yml task until the following issue is fixed: https://github.com/gitpod-io/gitpod/issues/5090
ENV DOTNET_ROOT=/tmp/dotnet
ENV PATH=$PATH:/tmp/dotnet

RUN sudo apt-get -y install librocksdb-dev