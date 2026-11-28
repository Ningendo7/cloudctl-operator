# AWS Emulator for make test-emulator. AWS publishes no image, only signed
# packages. The package is pinned by checksum, and its signature must come from
# the key AWS publishes the fingerprint of, or the build fails.
FROM public.ecr.aws/amazonlinux/amazonlinux:2023-minimal@sha256:def78a1e7b7c3dc3e6a840ec5b70c94dddcb97099117ae1ba34dc7a39b8ba61d

ARG PACKAGE
ARG PACKAGE_SHA256
ARG KEY_FINGERPRINT=98BCF336A3BE53B734E967C52E5BF49BD940F624

ADD --checksum=sha256:${PACKAGE_SHA256} https://downloads.aws.com/aws-emulator/packages/aws-emulator/LTS/${PACKAGE} /tmp/emulator.rpm
ADD https://downloads.aws.com/aws-emulator/packages/aws-emulator/LTS/${PACKAGE}.sig /tmp/emulator.rpm.sig
ADD https://downloads.aws.com/keys/gpg/current.asc /tmp/aws.asc

# gpg --verify against a keyring file needs no gpg-agent, which the minimal
# image lacks.
RUN set -eu; \
    gpg --batch --dearmor < /tmp/aws.asc > /tmp/aws.gpg; \
    GNUPGHOME="$(mktemp -d)" gpg --batch --no-default-keyring --keyring /tmp/aws.gpg \
        --status-fd 1 --verify /tmp/emulator.rpm.sig /tmp/emulator.rpm > /tmp/verify.txt; \
    grep -q "^\[GNUPG:\] VALIDSIG ${KEY_FINGERPRINT} " /tmp/verify.txt; \
    rpm -i /tmp/emulator.rpm; \
    rm -f /tmp/emulator.rpm /tmp/emulator.rpm.sig /tmp/aws.asc /tmp/aws.gpg /tmp/verify.txt; \
    mkdir -p /home/emulator; \
    chown 1000 /home/emulator

USER 1000
ENV HOME=/home/emulator
ENTRYPOINT ["aws-emulator"]
