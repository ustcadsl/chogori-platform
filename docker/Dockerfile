FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt upgrade -y
RUN apt install -y build-essential cmake vim gdb strace psmisc pkg-config python3 python3-pip git sed 
RUN apt install autoconf automake libglib2.0-dev libfabric-dev pandoc libncurses5-dev libdaxctl-dev libndctl-dev
CMD ["/bin/bash"]

