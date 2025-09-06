# --- Build Stage ---
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy Go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the application for a static release
# CGO_ENABLED=0 is important for a static binary in a minimal container
# -ldflags="-w -s" strips debug information to reduce binary size
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /kshark ./cmd/kshark

# --- Final Stage ---
FROM alpine:latest

# It's good practice to run as a non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Copy the compiled binary from the builder stage
COPY --from=builder /kshark /usr/local/bin/kshark

# Copy non-code assets required at runtime
COPY web/templates/ /app/web/templates/
COPY ai_config.json.example /app/ai_config.json
COPY client.properties.example /app/client.properties

# Set the working directory
WORKDIR /app

# Set the user
USER appuser

# Expose any ports the application might use (if applicable)
# EXPOSE 8080

# Define the entrypoint for the container
ENTRYPOINT ["kshark"]

# Default command can be set here if needed
# CMD ["--help"]
