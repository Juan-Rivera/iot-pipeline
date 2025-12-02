ARG BUILD_FROM
FROM $BUILD_FROM

COPY agent/ /usr/src/agent/
COPY run.sh /run.sh
RUN chmod a+x /run.sh

RUN pip install aiohttp websockets

CMD [ "/run.sh" ]
