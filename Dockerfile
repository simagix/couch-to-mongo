FROM gradle:6.8.1-jdk11 as builder
RUN mkdir -p /github.com/simagix/couch-to-mongo
ADD . /github.com/simagix/couch-to-mongo
WORKDIR /github.com/simagix/couch-to-mongo
RUN gradle build
FROM gradle:6.8.1-jre11
LABEL Ken Chen <ken.chen@simagix.com>
USER simagix
WORKDIR /dist
COPY --from=builder /github.com/simagix/couch-to-mongo/build/libs/couch-to-mongo-0.0.1.jar /dist/couch-to-mongo.jar
COPY --from=builder /github.com/simagix/couch-to-mongo/migration.properties /dist/migration.properties
CMD ["java", "-jar", "/dist/couch-to-mongo.jar"]
