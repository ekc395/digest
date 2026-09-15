# The jar is built OUTSIDE Docker — CI runs `./mvnw package` first, and this
# file only assembles the result. Build locally with:
#   ./mvnw package -DskipTests && docker build -t digest .

# ---- stage 1: explode the fat jar into Boot's four layers ----------------
FROM eclipse-temurin:25-jre AS builder
WORKDIR /build
COPY target/*.jar app.jar
# Boot 4 uses jarmode=tools (older versions used layertools).
# --launcher extracts JarLauncher so the runtime stage needs no fat jar.
RUN java -Djarmode=tools -jar app.jar extract --layers --launcher

# ---- stage 2: runtime ---------------------------------------------------
FROM eclipse-temurin:25-jre
WORKDIR /app

RUN useradd --system --uid 1001 --no-create-home --shell /usr/sbin/nologin digest

# Ordered least- to most-frequently-changed. Docker invalidates the cache from
# the first changed layer downward, so a code change rebuilds only the last
# ~36 KB instead of all 52 MB.
COPY --from=builder /build/app/dependencies/ ./
COPY --from=builder /build/app/spring-boot-loader/ ./
COPY --from=builder /build/app/snapshot-dependencies/ ./
COPY --from=builder /build/app/application/ ./

# Files stay root-owned; the app runs unprivileged and cannot rewrite its own
# code. Spring's temp files go to /tmp, which is world-writable.
USER digest

EXPOSE 8080

# MaxRAMPercentage: the JVM reads cgroup limits automatically, but defaults to
# only 25% of available memory for the heap. 75% suits a container running one
# process. Raise cautiously — non-heap (metaspace, threads, direct buffers)
# still needs room.
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "org.springframework.boot.loader.launch.JarLauncher"]
