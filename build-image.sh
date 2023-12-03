set -e

./gradlew :contube-all:shadowJar

docker build -t contube/contube-all . -f docker/Dockerfile
