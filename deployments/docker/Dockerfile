FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/taskforge-api ./cmd/taskforge-api && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/taskforge-worker ./cmd/taskforge-worker && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/taskforge-migrate ./cmd/taskforge-migrate

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/taskforge-api /taskforge-api
COPY --from=build /out/taskforge-worker /taskforge-worker
COPY --from=build /out/taskforge-migrate /taskforge-migrate
EXPOSE 8080
USER nonroot:nonroot
