# Alpine has an old version of GLIBC - so we'll use ubuntu
FROM            ubuntu:20.10


COPY            saga-player /saga-player

WORKDIR		/

ENTRYPOINT      [ "/saga-player", "-c", "/conf/config.yml" ]
