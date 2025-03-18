FROM docker.dragonflydb.io/dragonflydb/dragonfly:latest

RUN apt-get update && apt-get install -y openssh-server && apt-get clean
RUN echo 'root:passwd' | chpasswd
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config
EXPOSE 22
CMD ["service", "ssh", "start", "-D"]
