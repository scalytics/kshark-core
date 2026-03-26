---
layout: default
title: Deployment
nav_order: 4
---

# kshark Deployment Guide

**Version:** 1.0
**Last Updated:** 2025-11-13

---

## Table of Contents

1. [Overview](#overview)
2. [Local Deployment](#local-deployment)
3. [Docker Deployment](#docker-deployment)
4. [Kubernetes Deployment](#kubernetes-deployment)
5. [CI/CD Integration](#cicd-integration)
6. [Production Deployment](#production-deployment)
7. [Monitoring & Observability](#monitoring--observability)
8. [Deployment Improvements](#deployment-improvements)
9. [Best Practices](#best-practices)
10. [Troubleshooting](#troubleshooting)

---

## Overview

This guide covers deployment strategies for kshark across different environments, from local development to production Kubernetes clusters.

### Deployment Options Summary

| Environment | Deployment Method | Use Case |
|-------------|-------------------|----------|
| **Local** | Binary | Development, ad-hoc diagnostics |
| **Docker** | Container | Isolated execution, CI/CD |
| **Kubernetes** | CronJob/Job | Automated monitoring, scheduled checks |
| **CI/CD** | Pipeline integration | Pre-deployment validation |
| **Lambda/Functions** | Serverless | Event-driven diagnostics |

---

## Local Deployment

### Prerequisites

- Go 1.23+ (for building from source)
- Access to target Kafka cluster
- Configuration files prepared

### Option 1: Pre-built Binary

**Download:**
```bash
# Linux (amd64)
wget https://github.com/scalytics/kshark-core/releases/latest/download/kshark-linux-amd64.tar.gz
tar -xzf kshark-linux-amd64.tar.gz
sudo mv kshark /usr/local/bin/
chmod +x /usr/local/bin/kshark

# macOS (arm64)
wget https://github.com/scalytics/kshark-core/releases/latest/download/kshark-darwin-arm64.tar.gz
tar -xzf kshark-darwin-arm64.tar.gz
sudo mv kshark /usr/local/bin/
chmod +x /usr/local/bin/kshark

# Windows (amd64)
wget https://github.com/scalytics/kshark-core/releases/latest/download/kshark-windows-amd64.zip
unzip kshark-windows-amd64.zip
# Add to PATH or move to C:\Windows\System32\
```

**Verify Installation:**
```bash
kshark --version
```

---

### Option 2: Build from Source

**Clone and Build:**
```bash
# Clone repository
git clone https://github.com/scalytics/kshark-core.git
cd kshark-core

# Download dependencies
go mod download

# Build
go build -o kshark ./cmd/kshark

# Install (optional)
sudo mv kshark /usr/local/bin/
```

**Build with Version Information:**
```bash
VERSION=$(git describe --tags --always --dirty)
COMMIT=$(git rev-parse --short HEAD)
DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)

go build \
  -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
  -o kshark ./cmd/kshark
```

---

### Configuration

**Create Configuration Directory:**
```bash
mkdir -p ~/.kshark
chmod 700 ~/.kshark
```

**Create Properties File:**
```bash
cat > ~/.kshark/client.properties <<EOF
bootstrap.servers=broker.example.com:9092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.username=your-username
sasl.password=your-password
EOF

chmod 600 ~/.kshark/client.properties
```

**Create AI Config (Optional):**
```bash
cat > ~/.kshark/ai_config.json <<EOF
{
  "provider": "openai",
  "api_key": "sk-...",
  "api_endpoint": "https://api.openai.com/v1/chat/completions",
  "model": "gpt-4"
}
EOF

chmod 600 ~/.kshark/ai_config.json
```

---

### Usage

```bash
# Basic check
kshark -props ~/.kshark/client.properties

# With topic test
kshark -props ~/.kshark/client.properties -topic test-topic

# Automated mode
kshark -props ~/.kshark/client.properties -y
```

---

## Docker Deployment

### Building the Image

**Using Provided Dockerfile:**
```bash
# Clone repository
git clone https://github.com/scalytics/kshark-core.git
cd kshark-core

# Build image
docker build -t kshark:latest .

# Tag for registry
docker tag kshark:latest your-registry.com/kshark:latest

# Push to registry
docker push your-registry.com/kshark:latest
```

**Multi-platform Build:**
```bash
# Enable buildx
docker buildx create --use

# Build for multiple platforms
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t your-registry.com/kshark:latest \
  --push \
  .
```

---

### Running Containers

**Basic Run:**
```bash
docker run --rm \
  -v $(pwd)/client.properties:/config/client.properties:ro \
  kshark:latest -props /config/client.properties
```

**With Reports Output:**
```bash
mkdir -p reports

docker run --rm \
  -v $(pwd)/client.properties:/config/client.properties:ro \
  -v $(pwd)/reports:/app/reports \
  kshark:latest -props /config/client.properties -y
```

**With AI Analysis:**
```bash
docker run --rm \
  -v $(pwd)/client.properties:/config/client.properties:ro \
  -v $(pwd)/ai_config.json:/app/ai_config.json:ro \
  -v $(pwd)/license.key:/app/license.key:ro \
  -v $(pwd)/reports:/app/reports \
  kshark:latest -props /config/client.properties --analyze -y
```

---

### Docker Compose

**docker-compose.yml:**
```yaml
version: '3.8'

services:
  kshark:
    image: kshark:latest
    volumes:
      - ./config/client.properties:/config/client.properties:ro
      - ./config/ai_config.json:/app/ai_config.json:ro
      - ./secrets/license.key:/app/license.key:ro
      - ./reports:/app/reports
    command: ["-props", "/config/client.properties", "-y"]
    restart: "no"

  kshark-scheduler:
    image: kshark:latest
    volumes:
      - ./config/client.properties:/config/client.properties:ro
      - ./reports:/app/reports
    command: ["-props", "/config/client.properties", "-topic", "health-check", "-y"]
    restart: "no"
    # Use with cron or Kubernetes CronJob for scheduling
```

**Run:**
```bash
docker-compose up kshark
```

---

## Kubernetes Deployment

### ConfigMap for Configuration

**configmap.yaml:**
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kshark-config
  namespace: monitoring
data:
  client.properties: |
    bootstrap.servers=kafka-broker.kafka.svc.cluster.local:9092
    security.protocol=SASL_SSL
    sasl.mechanism=SCRAM-SHA-256
    # Do not put credentials here - use Secret instead
```

---

### Secret for Credentials

**secret.yaml:**
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kshark-credentials
  namespace: monitoring
type: Opaque
stringData:
  sasl.username: "your-api-key"
  sasl.password: "your-api-secret"
  client.properties: |
    bootstrap.servers=kafka-broker.kafka.svc.cluster.local:9092
    security.protocol=SASL_SSL
    sasl.mechanism=SCRAM-SHA-256
    sasl.username=your-username
    sasl.password=your-password
```

**Create Secret:**
```bash
kubectl create secret generic kshark-credentials \
  --from-file=client.properties=./client.properties \
  --from-file=ai_config.json=./ai_config.json \
  --from-file=license.key=./license.key \
  -n monitoring
```

---

### One-time Job

**job.yaml:**
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: kshark-diagnostic
  namespace: monitoring
spec:
  ttlSecondsAfterFinished: 300
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: kshark
        image: your-registry.com/kshark:latest
        args:
          - "-props"
          - "/config/client.properties"
          - "-topic"
          - "diagnostic-test"
          - "-y"
        volumeMounts:
        - name: config
          mountPath: /config
          readOnly: true
        - name: reports
          mountPath: /app/reports
        resources:
          requests:
            memory: "64Mi"
            cpu: "100m"
          limits:
            memory: "128Mi"
            cpu: "200m"
      volumes:
      - name: config
        secret:
          secretName: kshark-credentials
      - name: reports
        emptyDir: {}
```

**Deploy:**
```bash
kubectl apply -f job.yaml
```

**View Logs:**
```bash
kubectl logs -f job/kshark-diagnostic -n monitoring
```

---

### CronJob for Periodic Checks

**cronjob.yaml:**
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: kshark-health-check
  namespace: monitoring
spec:
  # Run every 15 minutes
  schedule: "*/15 * * * *"

  # Keep last 3 successful and 1 failed job
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 1

  jobTemplate:
    spec:
      # Clean up completed jobs after 10 minutes
      ttlSecondsAfterFinished: 600

      template:
        metadata:
          labels:
            app: kshark
            component: health-check
        spec:
          restartPolicy: OnFailure

          containers:
          - name: kshark
            image: your-registry.com/kshark:latest
            imagePullPolicy: IfNotPresent

            args:
              - "-props"
              - "/config/client.properties"
              - "-topic"
              - "health-check"
              - "-y"
              - "-timeout"
              - "30s"

            volumeMounts:
            - name: config
              mountPath: /config
              readOnly: true
            - name: reports
              mountPath: /app/reports

            resources:
              requests:
                memory: "64Mi"
                cpu: "100m"
              limits:
                memory: "128Mi"
                cpu: "200m"

            securityContext:
              runAsNonRoot: true
              runAsUser: 1000
              allowPrivilegeEscalation: false
              capabilities:
                drop:
                - ALL

          volumes:
          - name: config
            secret:
              secretName: kshark-credentials
              defaultMode: 0400
          - name: reports
            persistentVolumeClaim:
              claimName: kshark-reports
```

**Deploy:**
```bash
kubectl apply -f cronjob.yaml
```

**Trigger Manual Run:**
```bash
kubectl create job kshark-manual-$(date +%s) \
  --from=cronjob/kshark-health-check \
  -n monitoring
```

---

### PersistentVolumeClaim for Reports

**pvc.yaml:**
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: kshark-reports
  namespace: monitoring
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 10Gi
  storageClassName: standard
```

---

### Service Account (Optional)

**serviceaccount.yaml:**
```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kshark
  namespace: monitoring

---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: kshark-role
  namespace: monitoring
rules:
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get", "list"]
  resourceNames: ["kshark-credentials"]

---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: kshark-rolebinding
  namespace: monitoring
subjects:
- kind: ServiceAccount
  name: kshark
  namespace: monitoring
roleRef:
  kind: Role
  name: kshark-role
  apiGroup: rbac.authorization.k8s.io
```

---

## CI/CD Integration

### GitHub Actions

**workflow.yaml:**
```yaml
name: Kafka Connectivity Check

on:
  push:
    branches: [main, staging, production]
  pull_request:
    branches: [main]
  schedule:
    - cron: '0 */6 * * *'  # Every 6 hours

jobs:
  kafka-diagnostic:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Download kshark
        run: |
          wget https://github.com/scalytics/kshark-core/releases/latest/download/kshark-linux-amd64.tar.gz
          tar -xzf kshark-linux-amd64.tar.gz
          chmod +x kshark

      - name: Create configuration
        run: |
          cat > client.properties <<EOF
          bootstrap.servers=${{ secrets.KAFKA_BOOTSTRAP_SERVERS }}
          security.protocol=SASL_SSL
          sasl.mechanism=PLAIN
          sasl.username=${{ secrets.KAFKA_API_KEY }}
          sasl.password=${{ secrets.KAFKA_API_SECRET }}
          EOF

      - name: Run diagnostic
        run: |
          ./kshark -props client.properties -topic ci-test -y

      - name: Upload report
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: kshark-report
          path: reports/*.html
          retention-days: 30
```

---

### GitLab CI

**.gitlab-ci.yml:**
```yaml
stages:
  - test

kafka_diagnostic:
  stage: test
  image: alpine:latest

  before_script:
    - apk add --no-cache wget tar
    - wget https://github.com/scalytics/kshark-core/releases/latest/download/kshark-linux-amd64.tar.gz
    - tar -xzf kshark-linux-amd64.tar.gz
    - chmod +x kshark

  script:
    - |
      cat > client.properties <<EOF
      bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS}
      security.protocol=SASL_SSL
      sasl.mechanism=PLAIN
      sasl.username=${KAFKA_API_KEY}
      sasl.password=${KAFKA_API_SECRET}
      EOF
    - ./kshark -props client.properties -topic ci-test -y

  artifacts:
    when: always
    paths:
      - reports/*.html
    expire_in: 30 days

  rules:
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"
    - if: $CI_COMMIT_BRANCH == "main"
    - if: $CI_PIPELINE_SOURCE == "schedule"
```

---

### Jenkins

**Jenkinsfile:**
```groovy
pipeline {
    agent any

    environment {
        KSHARK_VERSION = 'latest'
    }

    stages {
        stage('Download kshark') {
            steps {
                sh '''
                    wget https://github.com/scalytics/kshark-core/releases/latest/download/kshark-linux-amd64.tar.gz
                    tar -xzf kshark-linux-amd64.tar.gz
                    chmod +x kshark
                '''
            }
        }

        stage('Create Configuration') {
            steps {
                withCredentials([
                    string(credentialsId: 'kafka-bootstrap-servers', variable: 'KAFKA_SERVERS'),
                    usernamePassword(credentialsId: 'kafka-credentials',
                                   usernameVariable: 'KAFKA_USER',
                                   passwordVariable: 'KAFKA_PASS')
                ]) {
                    sh '''
                        cat > client.properties <<EOF
bootstrap.servers=${KAFKA_SERVERS}
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.username=${KAFKA_USER}
sasl.password=${KAFKA_PASS}
EOF
                    '''
                }
            }
        }

        stage('Run Diagnostic') {
            steps {
                sh './kshark -props client.properties -topic ci-test -y'
            }
        }
    }

    post {
        always {
            archiveArtifacts artifacts: 'reports/*.html', allowEmptyArchive: true
        }
        failure {
            emailext (
                subject: "Kafka Connectivity Check Failed",
                body: "The Kafka connectivity diagnostic failed. Check the attached report.",
                attachmentsPattern: 'reports/*.html',
                to: 'devops@example.com'
            )
        }
    }
}
```

---

## Production Deployment

### High Availability Setup

**Considerations:**
- Run from multiple availability zones
- Use Kubernetes for orchestration
- Store reports in centralized storage (S3, GCS)
- Implement alerting on failures

**Multi-cluster Deployment:**
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: kshark-cluster-1
spec:
  schedule: "*/15 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: kshark
            image: kshark:latest
            args: ["-props", "/config/cluster-1.properties", "-y"]
            volumeMounts:
            - name: config
              mountPath: /config
          volumes:
          - name: config
            secret:
              secretName: cluster-1-credentials

---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: kshark-cluster-2
spec:
  schedule: "*/15 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: kshark
            image: kshark:latest
            args: ["-props", "/config/cluster-2.properties", "-y"]
            volumeMounts:
            - name: config
              mountPath: /config
          volumes:
          - name: config
            secret:
              secretName: cluster-2-credentials
```

---

### Report Storage

**AWS S3 Integration:**
```bash
# Run diagnostic and upload to S3
docker run --rm \
  -v $(pwd)/client.properties:/config/client.properties:ro \
  -v $(pwd)/reports:/app/reports \
  -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
  -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
  kshark:latest -props /config/client.properties -y

# Upload reports
aws s3 sync reports/ s3://my-bucket/kshark-reports/ \
  --exclude "*" \
  --include "*.html" \
  --include "*.json"
```

**Post-run Script:**
```bash
#!/bin/bash
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT_FILE="reports/analysis_report_${TIMESTAMP}.html"

# Run kshark
./kshark -props client.properties -y

# Upload to S3
if [ -f "$REPORT_FILE" ]; then
    aws s3 cp "$REPORT_FILE" "s3://my-bucket/kshark-reports/"
fi

# Send to logging system
if grep -q "FAIL" "$REPORT_FILE"; then
    curl -X POST https://logs.example.com/api/alerts \
         -H "Content-Type: application/json" \
         -d "{\"severity\":\"error\",\"message\":\"Kafka connectivity check failed\",\"report\":\"$REPORT_FILE\"}"
fi
```

---

## Monitoring & Observability

### Prometheus Metrics (Future Feature)

**Proposed Metrics:**
```
kshark_check_duration_seconds{layer="L3",status="OK"}
kshark_check_duration_seconds{layer="L4",status="OK"}
kshark_check_duration_seconds{layer="L5-6",status="OK"}
kshark_check_duration_seconds{layer="L7",status="OK"}

kshark_check_total{layer="L3",status="OK"} 1
kshark_check_total{layer="L3",status="FAIL"} 0

kshark_last_check_timestamp
```

---

### Alerting

**Alert on Failures:**
```yaml
# Kubernetes Event
apiVersion: v1
kind: ConfigMap
metadata:
  name: kshark-alert-script
data:
  alert.sh: |
    #!/bin/sh
    if grep -q "FAIL" /app/reports/*.html; then
        curl -X POST https://alerts.example.com/webhook \
             -H "Content-Type: application/json" \
             -d '{"text":"Kafka connectivity check failed"}'
    fi
```

**Slack Integration:**
```bash
#!/bin/bash
WEBHOOK_URL="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

# Run diagnostic
./kshark -props client.properties -y > /tmp/kshark.log 2>&1

# Check for failures
if grep -q "FAIL" /tmp/kshark.log; then
    FAILURES=$(grep "FAIL" /tmp/kshark.log)
    curl -X POST $WEBHOOK_URL \
         -H 'Content-Type: application/json' \
         -d "{\"text\":\"⚠️ Kafka Connectivity Alert\n\`\`\`$FAILURES\`\`\`\"}"
fi
```

---

## Deployment Improvements

### Recommended Enhancements

#### 1. Container Image Optimization

**Current State:**
- Base image: `alpine:latest`
- Final image size: ~50MB

**Improvements:**
```dockerfile
# Use specific version tags (not latest)
FROM golang:1.23.2-alpine3.19 AS builder

# Use distroless for even smaller runtime
FROM gcr.io/distroless/static:nonroot
COPY --from=builder /kshark /usr/local/bin/kshark
COPY --from=builder /app/web/templates/ /app/web/templates/
ENTRYPOINT ["kshark"]

# Result: ~20MB final image
```

**Benefits:**
- Smaller image size
- Fewer vulnerabilities
- Faster pulls

---

#### 2. Security Scanning

**Add to CI/CD:**
```yaml
# .github/workflows/security-scan.yml
name: Security Scan

on:
  push:
    branches: [main]
  pull_request:

jobs:
  trivy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Build image
        run: docker build -t kshark:${{ github.sha }} .

      - name: Run Trivy scan
        uses: aquasecurity/trivy-action@master
        with:
          image-ref: kshark:${{ github.sha }}
          format: 'sarif'
          output: 'trivy-results.sarif'

      - name: Upload results
        uses: github/codeql-action/upload-sarif@v2
        with:
          sarif_file: 'trivy-results.sarif'
```

---

#### 3. Helm Chart

**Chart.yaml:**
```yaml
apiVersion: v2
name: kshark
description: Kafka connectivity diagnostic tool
type: application
version: 1.0.0
appVersion: "1.0.0"
```

**values.yaml:**
```yaml
image:
  repository: your-registry.com/kshark
  tag: latest
  pullPolicy: IfNotPresent

schedule: "*/15 * * * *"

config:
  existingSecret: "kshark-credentials"
  topic: "health-check"
  timeout: "30s"

resources:
  requests:
    memory: "64Mi"
    cpu: "100m"
  limits:
    memory: "128Mi"
    cpu: "200m"

persistence:
  enabled: true
  storageClass: "standard"
  size: "10Gi"
```

**templates/cronjob.yaml:**
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: {{ include "kshark.fullname" . }}
spec:
  schedule: {{ .Values.schedule | quote }}
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: kshark
            image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
            imagePullPolicy: {{ .Values.image.pullPolicy }}
            args:
              - "-props"
              - "/config/client.properties"
              - "-topic"
              - {{ .Values.config.topic | quote }}
              - "-timeout"
              - {{ .Values.config.timeout | quote }}
              - "-y"
            resources:
              {{- toYaml .Values.resources | nindent 14 }}
          # ... (volumeMounts, volumes)
```

**Installation:**
```bash
helm install kshark ./helm/kshark \
  --namespace monitoring \
  --create-namespace \
  --set config.existingSecret=my-kafka-credentials
```

---

#### 4. Operator Pattern (Future)

**Custom Resource Definition:**
```yaml
apiVersion: diagnostics.kafka.io/v1alpha1
kind: KafkaHealthCheck
metadata:
  name: production-kafka-check
spec:
  schedule: "*/15 * * * *"
  target:
    bootstrapServers: "kafka.prod.svc.cluster.local:9092"
    credentialsSecret: "kafka-prod-credentials"
  checks:
    - type: connectivity
    - type: topic
      topicName: health-check
    - type: produce-consume
  ai:
    enabled: true
    provider: openai
  notifications:
    - type: slack
      webhook: https://hooks.slack.com/...
```

---

#### 5. Multi-Architecture Support

**Enhanced .goreleaser.yaml:**
```yaml
builds:
  - id: kshark
    main: ./cmd/kshark/main.go
    binary: kshark
    env:
      - CGO_ENABLED=0
    goos:
      - linux
      - windows
      - darwin
    goarch:
      - amd64
      - arm64
      - arm
    goarm:
      - "7"
    # Ignore specific combinations
    ignore:
      - goos: windows
        goarch: arm
```

---

## Best Practices

### Configuration Management

1. **Never commit secrets**
   ```bash
   # Use .gitignore
   echo "*.properties" >> .gitignore
   echo "ai_config.json" >> .gitignore
   echo "license.key" >> .gitignore
   ```

2. **Use environment variables**
   ```bash
   # In Kubernetes
   - name: KAFKA_PASSWORD
     valueFrom:
       secretKeyRef:
         name: kafka-credentials
         key: password
   ```

3. **Separate configs per environment**
   ```
   config/
   ├── dev.properties
   ├── staging.properties
   └── prod.properties
   ```

---

### Resource Limits

**Recommended Kubernetes Resources:**
```yaml
resources:
  requests:
    memory: "64Mi"
    cpu: "100m"
  limits:
    memory: "128Mi"
    cpu: "200m"
```

**For AI-enabled checks:**
```yaml
resources:
  requests:
    memory: "128Mi"
    cpu: "200m"
  limits:
    memory: "256Mi"
    cpu: "500m"
```

---

### Security

1. **Run as non-root**
   ```yaml
   securityContext:
     runAsNonRoot: true
     runAsUser: 1000
     allowPrivilegeEscalation: false
     capabilities:
       drop:
       - ALL
   ```

2. **Use read-only volumes**
   ```yaml
   volumeMounts:
   - name: config
     mountPath: /config
     readOnly: true
   ```

3. **Scan images regularly**
   ```bash
   trivy image your-registry.com/kshark:latest
   ```

---

## Troubleshooting

### Issue: Job fails in Kubernetes

**Check logs:**
```bash
kubectl logs -l app=kshark -n monitoring
```

**Common causes:**
- Missing Secret/ConfigMap
- Incorrect volume mounts
- Network policies blocking Kafka access
- Resource limits too low

---

### Issue: Reports not persisted

**Solution:**
- Ensure PVC is bound
- Check volume mount path
- Verify write permissions

```bash
kubectl get pvc -n monitoring
kubectl describe pvc kshark-reports -n monitoring
```

---

### Issue: Container crashes

**Check:**
```bash
kubectl describe pod <pod-name> -n monitoring
kubectl logs <pod-name> -n monitoring --previous
```

**Common causes:**
- OOMKilled (increase memory limit)
- CrashLoopBackOff (check configuration)
- ImagePullBackOff (verify image exists)

---

## Conclusion

This deployment guide provides comprehensive strategies for running kshark across various environments. Choose the deployment method that best fits your use case and infrastructure.

**Next Steps:**
1. Review [SECURITY.md](SECURITY.md) for security best practices
2. Check [FEATURES.md](FEATURES.md) for complete feature documentation
3. See [ARCHITECTURE.md](ARCHITECTURE.md) for system architecture details

---

**Document Version:** 1.0
**Author:** kshark Development Team
**Last Review:** 2025-11-13
**Next Review:** 2025-12-13
