# docker build --tag=kvlayer-java-proxy:0.1.0 . 
# docker run -d -p 7321:7321

FROM centos:centos6
MAINTAINER Diffeo <support@diffeo.com>

RUN yum install -y epel-release \
 && yum update -y \
 && yum clean all

RUN yum install -y java-1.7.0-openjdk

COPY kvlayer-java-proxy /kjp/
COPY target/ /kjp/target/

CMD ["/kjp/kvlayer-java-proxy", "--port=7321"]
