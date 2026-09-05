FROM golang:1.26-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# Static binaries, no libc, nothing to exploit but the program itself.
FROM scratch
COPY --from=build /out/tritium /out/tritium-cli /out/tritium-monitor /
USER 65534:65534
EXPOSE 8080
ENTRYPOINT ["/tritium"]
