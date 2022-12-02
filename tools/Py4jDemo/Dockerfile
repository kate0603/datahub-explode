FROM anapsix/alpine-java
COPY ./build/libs/ /tmp
WORKDIR /tmp
ENTRYPOINT ["java", "-jar" , "Py4jDemo-1.0-all.jar"]