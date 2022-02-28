FROM openjdk:11
COPY target/dashboardams2g-0.0.1-SNAPSHOT.jar /
ENTRYPOINT ["java","-jar","/dashboardams2g-0.0.1-SNAPSHOT.jar"]