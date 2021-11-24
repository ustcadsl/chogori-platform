FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive

RUN sed -i 's/archive.ubuntu.com/mirrors.ustc.edu.cn/g' /etc/apt/sources.list
RUN sed -i 's/security.ubuntu.com/mirrors.ustc.edu.cn/g' /etc/apt/sources.list

RUN apt update
RUN apt upgrade -y
RUN apt install -y build-essential cmake vim gdb strace psmisc pkg-config python3 python3-pip git sed

CMD ["/bin/bash"]

