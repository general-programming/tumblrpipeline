FROM alpine:edge

# Update packages and install setup requirements.
RUN apk --no-cache upgrade && \
	apk add --no-cache -X http://dl-cdn.alpinelinux.org/alpine/edge/testing python3 python3-dev gcc musl-dev postgresql-dev ca-certificates && \
	update-ca-certificates && \
	rm -rf /var/cache/apk/*

# Set WORKDIR to /src
WORKDIR /src

# Add and install Python modules
ADD requirements.txt /src/requirements.txt
RUN pip3 install -r requirements.txt

# Bundle app source
ADD . /src

# Install main module
RUN python3 setup.py install

# Expose web port
EXPOSE 5000

# Command
CMD ["gunicorn", "-b", "0.0.0.0:5000", "-k", "gevent", "-w", "4", "archives:app"]
