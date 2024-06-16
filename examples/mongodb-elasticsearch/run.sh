set -e

ES_NAR="tubes/pulsar-io-elastic-search-3.1.1.nar"

if [ ! -f "$ES_NAR" ]; then
    curl -L -o "$ES_NAR" "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-3.1.1/connectors/pulsar-io-elastic-search-3.1.1.nar"
fi

docker-compose up -d
sleep 10
docker-compose exec mongodb bash -c '/usr/local/bin/init-inventory.sh'

