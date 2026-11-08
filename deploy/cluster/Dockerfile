FROM golang:1.27.3 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/server/go.mod cmd/server/go.sum ./cmd/server/
WORKDIR /src/cmd/server
RUN go mod download
COPY . /src
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' -o /tinysql-server .
RUN mkdir /data && chown 65532:65532 /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /tinysql-server /tinysql-server
COPY --from=build --chown=65532:65532 /data /data
WORKDIR /data
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["/tinysql-server"]
