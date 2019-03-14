FROM alpine:latest

RUN apk add --no-cache gettext
COPY k2http docker/config.yaml_env docker/k2http-start.sh glide.lock /app/

WORKDIR /app
ENTRYPOINT ["./k2http-start.sh"]
