FROM ballerina/ballerina:2201.9.0

WORKDIR /app

COPY Ballerina.toml .
COPY main.bal .
COPY lib/ ./lib/

RUN bal build

EXPOSE 8084

CMD ["bal", "run"]
