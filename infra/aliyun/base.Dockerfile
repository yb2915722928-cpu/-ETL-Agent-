FROM scratch
ADD alpine-minirootfs-3.22.2-x86_64.tar.gz /
RUN printf '%s\n' 'https://mirrors.aliyun.com/alpine/v3.22/main' 'https://mirrors.aliyun.com/alpine/v3.22/community' > /etc/apk/repositories
CMD ["/bin/sh"]
