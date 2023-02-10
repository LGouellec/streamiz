# runtime image
FROM mcr.microsoft.com/dotnet/aspnet:7.0 AS base

# install rocks db dependencies
RUN echo "deb http://ftp.us.debian.org/debian stretch main contrib non-free" >> /etc/apt/sources.list
RUN apt update
RUN apt install -y libc6-dev bzip2 lz4 librocksdb-dev

# Build image
FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /source

# copy sln and csproj for restore
COPY *.sln ./
COPY *.props ./
COPY */*.csproj ./

# this line depends on the csprojs having the same name as the containing folders
RUN for file in $(ls *.csproj); do mkdir -p ${file%.*} && mv $file ${file%.*}; done

# restore to cache image
RUN dotnet restore

# copy the rest
COPY . ./

# run the tests
WORKDIR /source/Streamiz.Tests
RUN dotnet test --no-restore

# publish the app
WORKDIR /source/Streamiz.Demo
RUN dotnet publish --no-restore -c Release -o ../out

FROM base as final

# copy published app from build image
WORKDIR /app
COPY --from=build /source/out .

# that's all folks
EXPOSE 8060
ENV ASPNETCORE_URLS=http://+:8060
ENTRYPOINT [ "dotnet", "Streamiz.Demo.dll" ]