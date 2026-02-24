FROM scratch
COPY conduit /conduit
EXPOSE 8090
ENTRYPOINT ["/conduit"]
CMD ["serve"]
