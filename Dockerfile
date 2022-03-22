FROM openjdk:8-jdk-alpine

COPY target/update*.jar /updatetables.jar

CMD ["java","-jar","/updatetables.jar", "jdbc:postgresql://buku-dev.cwqvzn15jxyk.ap-southeast-1.rds.amazonaws.com:5432/auth_dev", "buku_dev", "Tassel-Babbling7-Dairy"]