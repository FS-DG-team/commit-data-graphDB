FROM tigergraph/tigergraph:latest

COPY my-entrypoint.sh /home/tigergraph/my-entrypoint.sh

ENTRYPOINT ["/bin/sh", "-c", "bash -c /home/tigergraph/my-entrypoint.sh"]