FROM ballerina/ballerina:2201.9.0

WORKDIR /app

# Copy source files and dependencies
COPY Ballerina.toml .
COPY main.bal .
COPY lib/ ./lib/

# Build the application
RUN bal build

# Expose port
EXPOSE 8080

# Run the service
CMD ["bal", "run"]