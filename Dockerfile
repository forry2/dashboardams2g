FROM openjdk:11
COPY target/cruscottoCurveGasRest-0.0.1-SNAPSHOT.jar /
ENTRYPOINT ["java","-jar","/cruscottoCurveGasRest-0.0.1-SNAPSHOT.jar"]