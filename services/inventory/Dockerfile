# syntax=docker/dockerfile:1
FROM node:20-alpine AS build
WORKDIR /src
RUN corepack enable && corepack prepare pnpm@9.15.9 --activate
COPY package.json pnpm-lock.yaml tsconfig.json ./
RUN pnpm install --frozen-lockfile
COPY src ./src
RUN pnpm run build

FROM node:20-alpine
RUN addgroup -g 65532 -S app && adduser -S -D -H -u 65532 -G app app
WORKDIR /app
COPY --from=build --chown=65532:65532 /src/dist ./dist
COPY --from=build --chown=65532:65532 /src/package.json ./
USER 65532:65532
ENV NODE_ENV=production
ENV MESH_SERVICE=inventory
CMD ["node", "dist/run.js"]
