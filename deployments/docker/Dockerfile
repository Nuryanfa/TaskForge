FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/taskforge ./cmd/taskforge

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/taskforge /taskforge
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/taskforge"]
