# The fanout server, containerized ONLY so Linux traffic control (tc/netem) can shape its egress.
# macOS has no netem, so the flaky-network axis needs a Linux network namespace.
#
# Deliberately NOT containerized: the producer, the load clients, and Redis. They stay on the host
# so that t_emit and t_recv are stamped by the SAME system clock (see src/clock.js) -- putting the
# producer in a container would reintroduce cross-machine clock skew, the exact error class the
# harness was fixed to eliminate. The container sits in the middle as the device under test.
FROM node:20-alpine

# iproute2 provides `tc`; without it netem cannot be applied inside the container.
RUN apk add --no-cache iproute2

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY src ./src

EXPOSE 3000
CMD ["node", "src/server.js"]
